import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { Kafka } from "kafkajs";
import { pool as paymentPool } from "../src/db.js";
import { pool as bookingPool } from "../../booking/src/db.js";
import { pool as inventoryPool } from "../../inventory/src/db.js";
import {
    executePayment,
    getPaymentByBookingId,
    createPaymentService,
} from "../src/index.js";
import { createOrGetBooking, getBookingById } from "../../booking/src/booking.js";
import { createBookingApp } from "../../booking/src/bookingApi.js";
import { OutboxRelay } from "../../booking/src/outboxRelay.js";
import {
    applyInventoryOutcome,
    applyPaymentOutcome,
    applySeatsConfirmed,
} from "../../booking/src/bookingEvents.js";
import { reserveSeats } from "../../inventory/src/reserveSeats.js";
import { confirmSeats } from "../../inventory/src/confirmSeats.js";
import { releaseSeats } from "../../inventory/src/releaseSeats.js";
import type {
    PaymentFailedEvent,
    PaymentSucceededEvent,
    ProcessPaymentCommand,
} from "../../../packages/common/src/messages.js";

const testRunId = `pay-test-${Date.now()}`;
const eventId = `event-${testRunId}`;

async function setupSeats(seatNos: string[]) {
    await inventoryPool.query(
        "INSERT INTO events (id, name) VALUES ($1, $2) ON CONFLICT (id) DO NOTHING",
        [eventId, `Event for ${testRunId}`],
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
    intervalMs = 200,
): Promise<void> {
    const start = Date.now();
    while (Date.now() - start < timeoutMs) {
        if (await predicate()) return;
        await new Promise((r) => setTimeout(r, intervalMs));
    }
    throw new Error(`Wait condition timed out after ${timeoutMs}ms`);
}

async function runUnitAndIdempotencyTests() {
    console.log("\n--- 1. Testing Payment DB Persistence & Idempotency ---");

    const bookingId1 = randomUUID();

    // 1. Success execution
    const res1 = await executePayment(paymentPool, {
        messageId: randomUUID(),
        bookingId: bookingId1,
        type: "ProcessPayment",
        occurredAt: new Date().toISOString(),
        payload: { amount: 120, currency: "USD" },
    });

    assert.equal(res1.isDuplicate, false);
    assert.equal(res1.payment.bookingId, bookingId1);
    assert.equal(res1.payment.status, "SUCCEEDED");
    assert.equal(res1.payment.amount, 120);
    assert.equal(res1.event.type, "PaymentSucceeded");

    const savedPayment1 = await getPaymentByBookingId(paymentPool, bookingId1);
    assert.ok(savedPayment1);
    assert.equal(savedPayment1.id, res1.payment.id);
    assert.equal(savedPayment1.status, "SUCCEEDED");
    console.log("✓ Succeeded payment persisted in payment_db");

    // 2. Duplicate execution with same bookingId (Idempotency)
    const res1Duplicate = await executePayment(paymentPool, {
        messageId: randomUUID(),
        bookingId: bookingId1,
        type: "ProcessPayment",
        occurredAt: new Date().toISOString(),
        payload: { amount: 120, currency: "USD" },
    });

    assert.equal(res1Duplicate.isDuplicate, true, "Duplicate call must return isDuplicate = true");
    assert.equal(res1Duplicate.payment.id, res1.payment.id, "Payment ID must match original");
    assert.equal(res1Duplicate.payment.status, "SUCCEEDED");
    assert.equal(res1Duplicate.event.type, "PaymentSucceeded");
    assert.equal(
        (res1Duplicate.event as PaymentSucceededEvent).payload.paymentId,
        res1.payment.id,
        "Event payload must reference original paymentId",
    );

    // Verify exactly 1 row exists
    const countRes1 = await paymentPool.query(
        "SELECT COUNT(*) AS count FROM payments WHERE booking_id = $1",
        [bookingId1],
    );
    assert.equal(Number(countRes1.rows[0].count), 1, "Exactly one payment record must exist in DB");
    console.log("✓ Duplicate payment execution produced no duplicate DB row or side effect");

    // 3. Failure execution
    const bookingIdFail = randomUUID();
    const resFail = await executePayment(paymentPool, {
        messageId: randomUUID(),
        bookingId: bookingIdFail,
        type: "ProcessPayment",
        occurredAt: new Date().toISOString(),
        payload: { amount: -25 },
    });

    assert.equal(resFail.isDuplicate, false);
    assert.equal(resFail.payment.status, "FAILED");
    assert.equal(resFail.payment.reason, "INVALID_AMOUNT");
    assert.equal(resFail.event.type, "PaymentFailed");

    const savedFail = await getPaymentByBookingId(paymentPool, bookingIdFail);
    assert.ok(savedFail);
    assert.equal(savedFail.status, "FAILED");
    assert.equal(savedFail.reason, "INVALID_AMOUNT");

    // 4. Duplicate failure execution
    const resFailDuplicate = await executePayment(paymentPool, {
        messageId: randomUUID(),
        bookingId: bookingIdFail,
        type: "ProcessPayment",
        occurredAt: new Date().toISOString(),
        payload: { amount: -25 },
    });
    assert.equal(resFailDuplicate.isDuplicate, true);
    assert.equal(resFailDuplicate.payment.id, resFail.payment.id);
    assert.equal(resFailDuplicate.event.type, "PaymentFailed");
    console.log("✓ Failed payment persisted and deduplicated correctly");

    // 5. Concurrent execution race condition
    const bookingIdRace = randomUUID();
    const concurrentCalls = Array.from({ length: 5 }, () =>
        executePayment(paymentPool, {
            messageId: randomUUID(),
            bookingId: bookingIdRace,
            type: "ProcessPayment",
            occurredAt: new Date().toISOString(),
            payload: { amount: 50 },
        }),
    );

    const raceResults = await Promise.all(concurrentCalls);
    const nonDuplicates = raceResults.filter((r) => !r.isDuplicate);
    assert.equal(nonDuplicates.length, 1, "Exactly one concurrent call must be recorded as non-duplicate");

    const raceCountRes = await paymentPool.query(
        "SELECT COUNT(*) AS count FROM payments WHERE booking_id = $1",
        [bookingIdRace],
    );
    assert.equal(Number(raceCountRes.rows[0].count), 1, "Concurrency survived; exactly one row exists in DB");
    console.log("✓ 5 concurrent calls resolved safely with exactly 1 DB record");
}

async function runEndToEndScenarios() {
    console.log("\n--- 2. Testing End-to-End Payment Scenarios via Kafka & HTTP ---");

    const kafka = new Kafka({
        clientId: `test-payment-e2e-${Date.now()}`,
        brokers: ["localhost:9092"],
    });

    const producer = kafka.producer();
    await producer.connect();

    // Start Booking Outbox Relay
    const relay = new OutboxRelay(bookingPool, producer, { pollIntervalMs: 300 });
    relay.start();

    // Start Booking HTTP API
    const bookingApp = createBookingApp(async () => {
        relay.trigger();
    });
    const server = bookingApp.listen(0);
    const address = server.address();
    const port = typeof address === "object" && address ? address.port : 3001;
    const baseUrl = `http://localhost:${port}`;

    // Start Booking Kafka Consumer
    const bookingConsumer = kafka.consumer({
        groupId: `booking-pay-test-group-${Date.now()}`,
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

    // Start Inventory Kafka Consumer
    const inventoryConsumer = kafka.consumer({
        groupId: `inventory-pay-test-group-${Date.now()}`,
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
                await producer.send({
                    topic: "inventory.events",
                    messages: [
                        {
                            key: command.bookingId,
                            value: JSON.stringify({
                                messageId: randomUUID(),
                                bookingId: command.bookingId,
                                type: res.success ? "SeatsReserved" : "SeatsReservationFailed",
                                occurredAt: new Date().toISOString(),
                                payload: {
                                    eventId: command.payload.eventId,
                                    seatNos: command.payload.seatNos,
                                    ...(res.success ? {} : { reason: res.reason }),
                                },
                            }),
                        },
                    ],
                });
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
        groupId: `payment-srv-test-group-${Date.now()}`,
    });
    await paymentService.start();

    // Allow consumer groups to join
    await new Promise((r) => setTimeout(r, 2000));

    try {
        // SCENARIO 1: NO-PAYMENT SCENARIO
        // User creates booking, seats are reserved, user never calls /pay
        console.log("\n--- Testing Scenario 1: No-Payment (Hold Remains, Never Auto-Paid) ---");
        const noPaySeat = ["NP1"];
        await setupSeats(noPaySeat);

        const noPayRes = await fetch(`${baseUrl}/bookings`, {
            method: "POST",
            headers: {
                "Content-Type": "application/json",
                "Idempotency-Key": `no-pay-${Date.now()}`,
            },
            body: JSON.stringify({
                userId: "user-nopay",
                eventId,
                seatNos: noPaySeat,
            }),
        });
        assert.equal(noPayRes.status, 202);
        const { bookingId: noPayBookingId } = (await noPayRes.json()) as { bookingId: string };

        // Wait until SEATS_HELD
        await waitFor(async () => {
            const b = await getBookingById(noPayBookingId);
            return b?.status === "SEATS_HELD";
        });

        // Wait a moment and verify NO payment record exists in payment_db
        await new Promise((r) => setTimeout(r, 1000));
        const paymentCheck = await getPaymentByBookingId(paymentPool, noPayBookingId);
        assert.equal(paymentCheck, null, "No payment record must exist without explicit /pay call");
        const bookingStatusCheck = await getBookingById(noPayBookingId);
        assert.equal(bookingStatusCheck?.status, "SEATS_HELD", "Booking must remain in SEATS_HELD");
        console.log("✓ No-payment scenario passed: Booking stays in SEATS_HELD and is never auto-paid");

        // SCENARIO 2: SUCCESS PAYMENT SCENARIO
        console.log("\n--- Testing Scenario 2: Successful Payment ---");
        const successSeat = ["SP1"];
        await setupSeats(successSeat);

        const succCreate = await fetch(`${baseUrl}/bookings`, {
            method: "POST",
            headers: {
                "Content-Type": "application/json",
                "Idempotency-Key": `succ-${Date.now()}`,
            },
            body: JSON.stringify({
                userId: "user-succ",
                eventId,
                seatNos: successSeat,
            }),
        });
        const { bookingId: succBookingId } = (await succCreate.json()) as { bookingId: string };

        await waitFor(async () => {
            const b = await getBookingById(succBookingId);
            return b?.status === "SEATS_HELD";
        });

        // Explicitly trigger payment
        const payRes = await fetch(`${baseUrl}/bookings/${succBookingId}/pay`, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ amount: 99.5 }),
        });
        assert.equal(payRes.status, 202);

        // Wait for Booking to become CONFIRMED
        await waitFor(async () => {
            const b = await getBookingById(succBookingId);
            return b?.status === "CONFIRMED";
        });

        // Verify Payment DB has recorded SUCCEEDED payment
        const succPayment = await getPaymentByBookingId(paymentPool, succBookingId);
        assert.ok(succPayment);
        assert.equal(succPayment.status, "SUCCEEDED");
        assert.equal(succPayment.amount, 99.5);
        console.log("✓ Success scenario passed: Payment DB records SUCCEEDED and booking confirmed");

        // SCENARIO 3: FAILURE PAYMENT SCENARIO
        console.log("\n--- Testing Scenario 3: Failed Payment ---");
        const failSeat = ["FP1"];
        await setupSeats(failSeat);

        const failCreate = await fetch(`${baseUrl}/bookings`, {
            method: "POST",
            headers: {
                "Content-Type": "application/json",
                "Idempotency-Key": `fail-${Date.now()}`,
            },
            body: JSON.stringify({
                userId: "user-fail",
                eventId,
                seatNos: failSeat,
            }),
        });
        const { bookingId: failBookingId } = (await failCreate.json()) as { bookingId: string };

        await waitFor(async () => {
            const b = await getBookingById(failBookingId);
            return b?.status === "SEATS_HELD";
        });

        // Trigger payment with simulated decline
        const failPayRes = await fetch(`${baseUrl}/bookings/${failBookingId}/pay`, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ amount: -10 }),
        });
        assert.equal(failPayRes.status, 202);

        // Wait for compensation to cancel booking and release seats
        await waitFor(async () => {
            const b = await getBookingById(failBookingId);
            return b?.status === "CANCELLED";
        });

        await waitFor(async () => {
            const s = await getInventorySeat("FP1");
            return s.status === "AVAILABLE" && s.held_by_booking === null;
        });

        // Verify Payment DB has recorded FAILED payment
        const failPayment = await getPaymentByBookingId(paymentPool, failBookingId);
        assert.ok(failPayment);
        assert.equal(failPayment.status, "FAILED");
        assert.equal(failPayment.reason, "INVALID_AMOUNT");
        console.log("✓ Failure scenario passed: Payment DB records FAILED, booking cancelled, seats released");

        // SCENARIO 4: DUPLICATE PAYMENT SCENARIO
        console.log("\n--- Testing Scenario 4: Duplicate Payment Calls (No Duplicate Side Effect) ---");
        // Re-call /pay on succBookingId (which is already CONFIRMED)
        const dupPayRes = await fetch(`${baseUrl}/bookings/${succBookingId}/pay`, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ amount: 99.5 }),
        });

        assert.equal(dupPayRes.status, 200, "Duplicate payment on already paid/confirmed booking returns 200 OK");
        const dupJson = (await dupPayRes.json()) as { bookingId: string; status: string; duplicate: boolean };
        assert.equal(dupJson.bookingId, succBookingId);
        assert.equal(dupJson.status, "CONFIRMED");
        assert.equal(dupJson.duplicate, true);

        // Verify payment_db STILL has exactly ONE record for succBookingId
        const succCount = await paymentPool.query(
            "SELECT COUNT(*) AS count FROM payments WHERE booking_id = $1",
            [succBookingId],
        );
        assert.equal(Number(succCount.rows[0].count), 1, "Duplicate payment attempt did not create extra row");
        console.log("✓ Duplicate payment scenario passed: HTTP returns duplicate confirmation, DB retains exactly 1 payment record");

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
        await setupSeats(["NP1", "SP1", "FP1"]);
        await runUnitAndIdempotencyTests();
        await runEndToEndScenarios();

        console.log("\n========================================================");
        console.log("ALL PHASE 7 PAYMENT TESTS PASSED!");
        console.log("========================================================\n");
        process.exit(0);
    } catch (error) {
        console.error("\n❌ Phase 7 payment test failed:", error);
        process.exit(1);
    } finally {
        await paymentPool.end();
        await bookingPool.end();
        await inventoryPool.end();
    }
}

main().catch((err) => {
    console.error("Fatal test error:", err);
    process.exit(1);
});
