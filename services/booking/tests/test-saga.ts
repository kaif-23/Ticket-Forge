import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { Kafka } from "kafkajs";
import { pool as bookingPool } from "../src/db.js";
import { pool as inventoryPool } from "../../inventory/src/db.js";
import {
    createOrGetBooking,
    getBookingById,
    payBooking,
    BookingNotFoundError,
    InvalidBookingStateError,
} from "../src/booking.js";
import {
    applyInventoryOutcome,
    applyPaymentOutcome,
    applySeatsConfirmed,
} from "../src/bookingEvents.js";
import { getSagaLog } from "../src/sagaLog.js";
import { createBookingApp } from "../src/bookingApi.js";
import { OutboxRelay } from "../src/outboxRelay.js";
import { createPaymentService } from "../../payment/src/index.js";
import { reserveSeats } from "../../inventory/src/reserveSeats.js";
import { confirmSeats } from "../../inventory/src/confirmSeats.js";
import { releaseSeats } from "../../inventory/src/releaseSeats.js";

const testRunId = `saga-${Date.now()}`;
const eventId = `event-${testRunId}`;

async function setupInventoryEventAndSeats(seatNos: string[]) {
    await inventoryPool.query(
        "INSERT INTO events (id, name) VALUES ($1, $2) ON CONFLICT (id) DO NOTHING",
        [eventId, `Test Event ${testRunId}`],
    );

    for (const seatNo of seatNos) {
        await inventoryPool.query(
            `INSERT INTO seats (event_id, seat_no, status, version)
             VALUES ($1, $2, 'AVAILABLE', 0)
             ON CONFLICT (event_id, seat_no) DO UPDATE
             SET status = 'AVAILABLE', held_by_booking = NULL, hold_expires_at = NULL, version = 0`,
            [eventId, seatNo],
        );
    }
}

async function getInventorySeat(seatNo: string) {
    const res = await inventoryPool.query<{
        status: string;
        held_by_booking: string | null;
    }>(
        "SELECT status, held_by_booking FROM seats WHERE event_id = $1 AND seat_no = $2",
        [eventId, seatNo],
    );
    return res.rows[0];
}

async function waitFor(
    predicate: () => Promise<boolean>,
    timeoutMs = 15000,
    intervalMs = 250,
): Promise<void> {
    const start = Date.now();
    while (Date.now() - start < timeoutMs) {
        if (await predicate()) return;
        await new Promise((resolve) => setTimeout(resolve, intervalMs));
    }
    throw new Error(`Condition timed out after ${timeoutMs}ms`);
}

async function runSagaUnitAndStateGuardTests() {
    console.log("\n--- 1. Testing Saga State Guards & Transitions ---");

    const bookingResult = await createOrGetBooking(
        {
            userId: "guard-user",
            eventId,
            seatNos: ["SG1"],
        },
        `guard-key-${Date.now()}`,
    );
    const bookingId = bookingResult.booking.id;

    // 1. Cannot pay when status is PENDING
    let caughtPendingPay = false;
    try {
        await payBooking(bookingId);
    } catch (err) {
        if (err instanceof InvalidBookingStateError) {
            caughtPendingPay = true;
        }
    }
    assert.equal(caughtPendingPay, true, "Cannot pay for a booking in PENDING state");

    // 2. Paying nonexistent booking throws BookingNotFoundError
    let caughtNotFound = false;
    try {
        await payBooking(randomUUID());
    } catch (err) {
        if (err instanceof BookingNotFoundError) {
            caughtNotFound = true;
        }
    }
    assert.equal(caughtNotFound, true, "Paying nonexistent booking must throw BookingNotFoundError");

    // 3. Stale events do not alter state
    const stalePayment = await applyPaymentOutcome({
        messageId: randomUUID(),
        bookingId,
        type: "PaymentSucceeded",
        occurredAt: new Date().toISOString(),
        payload: { paymentId: "pay-1", amount: 100 },
    });
    assert.equal(stalePayment.kind, "stale", "Payment outcome on PENDING booking must be considered stale");

    const staleConfirmation = await applySeatsConfirmed({
        messageId: randomUUID(),
        bookingId,
        type: "SeatsConfirmed",
        occurredAt: new Date().toISOString(),
        payload: { eventId, seatNos: ["SG1"] },
    });
    assert.equal(staleConfirmation.kind, "stale", "SeatsConfirmed on PENDING booking must be considered stale");

    console.log("✓ State guards and stale event handling verified");
}

async function runRealKafkaDistributedSagaTests() {
    console.log("\n--- 2. Testing End-to-End Distributed Saga via Kafka ---");

    const kafka = new Kafka({
        clientId: `saga-orchestrator-test-${Date.now()}`,
        brokers: ["localhost:9092"],
    });

    const producer = kafka.producer();
    await producer.connect();

    // Start Outbox Relay for Booking
    const relay = new OutboxRelay(bookingPool, producer, {
        pollIntervalMs: 300,
    });
    relay.start();

    // Start Booking HTTP API with trigger
    const app = createBookingApp(async () => {
        relay.trigger();
    });

    const server = app.listen(0);
    const address = server.address();
    const port = typeof address === "object" && address ? address.port : 3001;
    const baseUrl = `http://localhost:${port}`;

    // Start Booking Kafka Consumer (listening to inventory.events and payment.events)
    const bookingConsumer = kafka.consumer({
        groupId: `booking-saga-test-group-${Date.now()}`,
    });
    await bookingConsumer.connect();
    await bookingConsumer.subscribe({
        topics: ["inventory.events", "payment.events"],
        fromBeginning: false,
    });

    await bookingConsumer.run({
        eachMessage: async ({ topic, message }) => {
            if (!message.value) return;
            const event = JSON.parse(message.value.toString());

            if (topic === "payment.events") {
                const res = await applyPaymentOutcome(event);
                if (res.kind === "updated") relay.trigger();
            } else if (topic === "inventory.events") {
                if (event.type === "SeatsConfirmed") {
                    const res = await applySeatsConfirmed(event);
                    if (res.kind === "updated") relay.trigger();
                } else if (event.type === "SeatsReserved" || event.type === "SeatsReservationFailed") {
                    const res = await applyInventoryOutcome(event);
                    if (res.kind === "updated") relay.trigger();
                }
            }
        },
    });

    // Start Inventory Kafka Consumer (listening to inventory.commands)
    const inventoryConsumer = kafka.consumer({
        groupId: `inventory-saga-test-group-${Date.now()}`,
    });
    await inventoryConsumer.connect();
    await inventoryConsumer.subscribe({
        topic: "inventory.commands",
        fromBeginning: false,
    });

    await inventoryConsumer.run({
        eachMessage: async ({ message }) => {
            if (!message.value) return;
            const command = JSON.parse(message.value.toString());

            if (command.type === "ReserveSeats") {
                const res = await reserveSeats(
                    command.bookingId,
                    command.payload.eventId,
                    command.payload.seatNos,
                );
                if (res.success) {
                    await producer.send({
                        topic: "inventory.events",
                        messages: [
                            {
                                key: command.bookingId,
                                value: JSON.stringify({
                                    messageId: randomUUID(),
                                    bookingId: command.bookingId,
                                    type: "SeatsReserved",
                                    occurredAt: new Date().toISOString(),
                                    payload: {
                                        eventId: command.payload.eventId,
                                        seatNos: command.payload.seatNos,
                                    },
                                }),
                            },
                        ],
                    });
                } else {
                    await producer.send({
                        topic: "inventory.events",
                        messages: [
                            {
                                key: command.bookingId,
                                value: JSON.stringify({
                                    messageId: randomUUID(),
                                    bookingId: command.bookingId,
                                    type: "SeatsReservationFailed",
                                    occurredAt: new Date().toISOString(),
                                    payload: {
                                        eventId: command.payload.eventId,
                                        seatNos: command.payload.seatNos,
                                        reason: res.reason,
                                    },
                                }),
                            },
                        ],
                    });
                }
            } else if (command.type === "ConfirmSeats") {
                const res = await confirmSeats(
                    command.bookingId,
                    command.payload.eventId,
                    command.payload.seatNos,
                );
                if (res.success) {
                    await producer.send({
                        topic: "inventory.events",
                        messages: [
                            {
                                key: command.bookingId,
                                value: JSON.stringify({
                                    messageId: randomUUID(),
                                    bookingId: command.bookingId,
                                    type: "SeatsConfirmed",
                                    occurredAt: new Date().toISOString(),
                                    payload: {
                                        eventId: command.payload.eventId,
                                        seatNos: command.payload.seatNos,
                                    },
                                }),
                            },
                        ],
                    });
                }
            } else if (command.type === "ReleaseSeats") {
                const res = await releaseSeats(
                    command.bookingId,
                    command.payload.eventId,
                    command.payload.seatNos,
                );
                if (res.success) {
                    await producer.send({
                        topic: "inventory.events",
                        messages: [
                            {
                                key: command.bookingId,
                                value: JSON.stringify({
                                    messageId: randomUUID(),
                                    bookingId: command.bookingId,
                                    type: "SeatsReleased",
                                    occurredAt: new Date().toISOString(),
                                    payload: {
                                        eventId: command.payload.eventId,
                                        seatNos: command.payload.seatNos,
                                    },
                                }),
                            },
                        ],
                    });
                }
            }
        },
    });

    // Start Payment Microservice
    const paymentService = createPaymentService({
        groupId: `payment-saga-test-group-${Date.now()}`,
    });
    await paymentService.start();

    // Allow consumer groups to join and get partition assignments
    await new Promise((r) => setTimeout(r, 2000));

    try {
        // ==========================================
        // TEST A: HAPPY PATH SAGA WORKFLOW
        // PENDING -> SEATS_HELD -> PAID -> CONFIRMED
        // ==========================================
        console.log("\n--- Testing Happy Path Saga Workflow ---");
        const happySeatNos = ["H1", "H2"];
        await setupInventoryEventAndSeats(happySeatNos);

        // 1. Client creates booking
        const createRes = await fetch(`${baseUrl}/bookings`, {
            method: "POST",
            headers: {
                "Content-Type": "application/json",
                "Idempotency-Key": `happy-${Date.now()}`,
            },
            body: JSON.stringify({
                userId: "user-happy",
                eventId,
                seatNos: happySeatNos,
            }),
        });

        assert.equal(createRes.status, 202);
        const { bookingId: happyBookingId } = (await createRes.json()) as { bookingId: string };
        assert.ok(happyBookingId);

        // 2. Wait for Inventory reservation and transition to SEATS_HELD
        await waitFor(async () => {
            const b = await getBookingById(happyBookingId);
            return b?.status === "SEATS_HELD";
        });
        console.log("✓ Step 1: Booking transitioned to SEATS_HELD via ReserveSeats -> SeatsReserved");

        const heldSeat = await getInventorySeat("H1");
        assert.equal(heldSeat.status, "HELD");
        assert.equal(heldSeat.held_by_booking, happyBookingId);

        // 3. User triggers payment (ADR-007)
        const payRes = await fetch(`${baseUrl}/bookings/${happyBookingId}/pay`, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ amount: 150, currency: "USD" }),
        });
        assert.equal(payRes.status, 202);
        console.log("✓ Step 2: Payment requested via POST /bookings/:id/pay");

        // 4. Wait for full Saga completion: SEATS_HELD -> PAID -> CONFIRMED
        await waitFor(async () => {
            const b = await getBookingById(happyBookingId);
            return b?.status === "CONFIRMED";
        });
        console.log("✓ Step 3: Booking reached CONFIRMED via ProcessPayment -> PaymentSucceeded -> ConfirmSeats -> SeatsConfirmed");

        // 5. Verify authoritative Inventory database
        const soldSeat1 = await getInventorySeat("H1");
        const soldSeat2 = await getInventorySeat("H2");
        assert.equal(soldSeat1.status, "SOLD", "Seat H1 must be SOLD in inventory DB");
        assert.equal(soldSeat2.status, "SOLD", "Seat H2 must be SOLD in inventory DB");
        assert.equal(soldSeat1.held_by_booking, null);

        // 6. Verify Saga Log via GET /bookings/:id/saga
        const sagaRes = await fetch(`${baseUrl}/bookings/${happyBookingId}/saga`);
        assert.equal(sagaRes.status, 200);
        const sagaData = (await sagaRes.json()) as { history: Array<{ state: string; lastEvent: string }> };
        const states = sagaData.history.map((h) => h.state);
        assert.deepEqual(states, [
            "PENDING",
            "SEATS_HELD",
            "SEATS_HELD", // ProcessPayment step
            "PAID",
            "CONFIRMED",
        ]);
        console.log("✓ Step 4: Saga log audit trail verified for happy path:", states);

        // ==========================================
        // TEST B: PAYMENT FAILURE & COMPENSATION
        // PaymentFailed -> ReleaseSeats -> CANCELLED
        // ==========================================
        console.log("\n--- Testing Payment Failure Compensation Workflow ---");
        const compSeatNos = ["C1", "C2"];
        await setupInventoryEventAndSeats(compSeatNos);

        // 1. Client creates booking
        const compCreateRes = await fetch(`${baseUrl}/bookings`, {
            method: "POST",
            headers: {
                "Content-Type": "application/json",
                "Idempotency-Key": `comp-${Date.now()}`,
            },
            body: JSON.stringify({
                userId: "user-comp",
                eventId,
                seatNos: compSeatNos,
            }),
        });

        assert.equal(compCreateRes.status, 202);
        const { bookingId: compBookingId } = (await compCreateRes.json()) as { bookingId: string };

        // 2. Wait for reservation to reach SEATS_HELD
        await waitFor(async () => {
            const b = await getBookingById(compBookingId);
            return b?.status === "SEATS_HELD";
        });

        const compHeldSeat = await getInventorySeat("C1");
        assert.equal(compHeldSeat.status, "HELD");
        assert.equal(compHeldSeat.held_by_booking, compBookingId);

        // 3. Trigger payment with failure (e.g. simulateFailure: true or amount: -1)
        const compPayRes = await fetch(`${baseUrl}/bookings/${compBookingId}/pay`, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ amount: -50 }),
        });
        assert.equal(compPayRes.status, 202);
        console.log("✓ Step 1: Payment triggered with invalid amount (< 0) to induce failure");

        // 4. Wait for compensation: Booking transitions to CANCELLED and outboxes ReleaseSeats
        await waitFor(async () => {
            const b = await getBookingById(compBookingId);
            return b?.status === "CANCELLED";
        });
        console.log("✓ Step 2: PaymentFailed handled; Booking state transitioned to CANCELLED");

        // 5. Wait for Inventory compensation to execute (ReleaseSeats -> SeatsReleased)
        await waitFor(async () => {
            const s = await getInventorySeat("C1");
            return s.status === "AVAILABLE" && s.held_by_booking === null;
        });

        const releasedSeat2 = await getInventorySeat("C2");
        assert.equal(releasedSeat2.status, "AVAILABLE");
        assert.equal(releasedSeat2.held_by_booking, null);
        console.log("✓ Step 3: Compensating ReleaseSeats successfully executed; seats returned to AVAILABLE");

        // 6. Verify Saga Log for compensation path
        const compSagaRes = await fetch(`${baseUrl}/bookings/${compBookingId}/saga`);
        assert.equal(compSagaRes.status, 200);
        const compSagaData = (await compSagaRes.json()) as { history: Array<{ state: string; lastEvent: string }> };
        const compStates = compSagaData.history.map((h) => h.state);
        assert.deepEqual(compStates, [
            "PENDING",
            "SEATS_HELD",
            "SEATS_HELD", // ProcessPayment step
            "CANCELLED",
        ]);
        console.log("✓ Step 4: Saga log audit trail verified for compensation path:", compStates);

        // ==========================================
        // TEST C: INVENTORY SEAT UNAVAILABLE SAGA
        // PENDING -> CANCELLED
        // ==========================================
        console.log("\n--- Testing Inventory Reservation Failure Workflow ---");
        // Try booking already sold seats (H1, H2)
        const unavailCreateRes = await fetch(`${baseUrl}/bookings`, {
            method: "POST",
            headers: {
                "Content-Type": "application/json",
                "Idempotency-Key": `unavail-${Date.now()}`,
            },
            body: JSON.stringify({
                userId: "user-unavail",
                eventId,
                seatNos: ["H1"], // Already SOLD
            }),
        });

        assert.equal(unavailCreateRes.status, 202);
        const { bookingId: unavailBookingId } = (await unavailCreateRes.json()) as { bookingId: string };

        await waitFor(async () => {
            const b = await getBookingById(unavailBookingId);
            return b?.status === "CANCELLED";
        });
        console.log("✓ Reservation failure directly transitions PENDING to CANCELLED");

        const unavailSaga = await getSagaLog(bookingPool, unavailBookingId);
        assert.equal(unavailSaga[unavailSaga.length - 1].state, "CANCELLED");
        assert.equal(unavailSaga[unavailSaga.length - 1].lastEvent, "SeatsReservationFailed");

    } finally {
        await relay.stop();
        server.close();
        await bookingConsumer.disconnect();
        await inventoryConsumer.disconnect();
        await paymentService.stop();
        await producer.disconnect();
    }
}

async function main() {
    try {
        await setupInventoryEventAndSeats(["SG1"]);
        await runSagaUnitAndStateGuardTests();
        await runRealKafkaDistributedSagaTests();

        console.log("\n========================================================");
        console.log("ALL PHASE 6 SAGA ORCHESTRATION INTEGRATION TESTS PASSED!");
        console.log("========================================================\n");
        process.exit(0);
    } catch (error) {
        console.error("\n❌ Saga orchestration test failed:", error);
        process.exit(1);
    } finally {
        await bookingPool.end();
        await inventoryPool.end();
    }
}

main().catch((err) => {
    console.error("Fatal test error:", err);
    process.exit(1);
});
