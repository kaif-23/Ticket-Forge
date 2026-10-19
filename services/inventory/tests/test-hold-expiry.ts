import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { Kafka } from "kafkajs";
import { pool as inventoryPool } from "../src/db.js";
import { pool as bookingPool } from "../../booking/src/db.js";
import { ExpirySweeper } from "../src/expirySweeper.js";
import { reserveSeats } from "../src/reserveSeats.js";
import { createOrGetBooking, getBookingById } from "../../booking/src/booking.js";
import { createBookingApp } from "../../booking/src/bookingApi.js";
import { OutboxRelay } from "../../booking/src/outboxRelay.js";
import {
    applyHoldExpired,
    applyInventoryOutcome,
} from "../../booking/src/bookingEvents.js";
import { getSagaLog } from "../../booking/src/sagaLog.js";
import type { HoldExpiredEvent } from "../../../packages/common/src/messages.js";

const testRunId = `expiry-${Date.now()}`;
const eventId = `event-${testRunId}`;

async function setupSeats(seatNos: string[]) {
    await inventoryPool.query(
        "INSERT INTO events (id, name) VALUES ($1, $2) ON CONFLICT (id) DO NOTHING",
        [eventId, `Event ${testRunId}`],
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

async function getSeat(seatNo: string) {
    const res = await inventoryPool.query<{
        status: string;
        held_by_booking: string | null;
        hold_expires_at: Date | null;
    }>(
        "SELECT status, held_by_booking, hold_expires_at FROM seats WHERE event_id = $1 AND seat_no = $2",
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
    throw new Error(`Condition timed out after ${timeoutMs}ms`);
}

async function testSweeperUnitAndKafkaPublication() {
    console.log("\n--- 1. Testing ExpirySweeper Unit & Event Publication ---");

    const kafka = new Kafka({
        clientId: `test-sweeper-unit-${Date.now()}`,
        brokers: ["localhost:9092"],
    });

    const producer = kafka.producer();
    const consumer = kafka.consumer({
        groupId: `sweeper-test-group-${Date.now()}`,
    });

    await producer.connect();
    await consumer.connect();
    await consumer.subscribe({
        topic: "inventory.events",
        fromBeginning: false,
    });

    const capturedHoldExpiredEvents: HoldExpiredEvent[] = [];
    await consumer.run({
        eachMessage: async ({ message }) => {
            if (!message.value) return;
            const parsed = JSON.parse(message.value.toString());
            if (parsed.type === "HoldExpired") {
                capturedHoldExpiredEvents.push(parsed as HoldExpiredEvent);
            }
        },
    });

    // Wait for consumer assignment
    await new Promise((r) => setTimeout(r, 1500));

    const bookingExpired = `b-exp-${Date.now()}`;
    const bookingActive = `b-act-${Date.now()}`;

    await setupSeats(["E1", "E2", "A1"]);

    // Reserve E1, E2 for bookingExpired
    const r1 = await reserveSeats(bookingExpired, eventId, ["E1", "E2"]);
    assert.equal(r1.success, true);

    // Reserve A1 for bookingActive
    const r2 = await reserveSeats(bookingActive, eventId, ["A1"]);
    assert.equal(r2.success, true);

    // Manually expire bookingExpired's seats
    await inventoryPool.query(
        "UPDATE seats SET hold_expires_at = NOW() - INTERVAL '5 minutes' WHERE event_id = $1 AND seat_no = ANY($2::text[])",
        [eventId, ["E1", "E2"]],
    );

    // Instantiate Sweeper
    const sweeper = new ExpirySweeper(inventoryPool, producer, { sweepIntervalMs: 500 });
    const sweepResult = await sweeper.sweepOnce();

    assert.ok(sweepResult.expiredSeatsCount >= 2);
    const expiredForThisBooking = sweepResult.expiredBookings.find((b) => b.bookingId === bookingExpired);
    assert.ok(expiredForThisBooking);
    assert.equal(expiredForThisBooking.eventId, eventId);
    assert.deepEqual(expiredForThisBooking.seatNos.sort(), ["E1", "E2"]);

    // Verify database state: E1, E2 are AVAILABLE, A1 is still HELD
    const sE1 = await getSeat("E1");
    const sE2 = await getSeat("E2");
    const sA1 = await getSeat("A1");

    assert.equal(sE1.status, "AVAILABLE");
    assert.equal(sE1.held_by_booking, null);
    assert.equal(sE1.hold_expires_at, null);

    assert.equal(sE2.status, "AVAILABLE");
    assert.equal(sE2.held_by_booking, null);

    assert.equal(sA1.status, "HELD", "Active unexpired seat must remain HELD");
    assert.equal(sA1.held_by_booking, bookingActive);

    // Wait for Kafka event publication
    await waitFor(async () => {
        return capturedHoldExpiredEvents.some((e) => e.bookingId === bookingExpired);
    });

    const event = capturedHoldExpiredEvents.find((e) => e.bookingId === bookingExpired)!;
    assert.equal(event.type, "HoldExpired");
    assert.equal(event.payload.eventId, eventId);
    assert.deepEqual(event.payload.seatNos.sort(), ["E1", "E2"]);
    console.log("✓ Sweeper expired seats, cleared metadata, and published HoldExpired onto Kafka");

    // Repeat sweep should be idempotent and find 0 additional seats
    const repeatSweep = await sweeper.sweepOnce();
    const repeatExpiredThisEvent = repeatSweep.expiredBookings.filter((b) => b.eventId === eventId);
    assert.deepEqual(repeatExpiredThisEvent, []);
    console.log("✓ Repeat sweep is a no-op");

    await consumer.disconnect();
    await producer.disconnect();
}

async function testEndToEndHoldExpiryWorkflow() {
    console.log("\n--- 2. Testing End-to-End Hold Expiry & Booking Transition via Kafka ---");

    const kafka = new Kafka({
        clientId: `test-e2e-expiry-${Date.now()}`,
        brokers: ["localhost:9092"],
    });

    const producer = kafka.producer();
    await producer.connect();

    // Start Booking Outbox Relay
    const relay = new OutboxRelay(bookingPool, producer, { pollIntervalMs: 300 });
    relay.start();

    // Start Booking HTTP API
    const app = createBookingApp(async () => {
        relay.trigger();
    });
    const server = app.listen(0);
    const address = server.address();
    const port = typeof address === "object" && address ? address.port : 3001;
    const baseUrl = `http://localhost:${port}`;

    // Start Booking Kafka Consumer
    const bookingConsumer = kafka.consumer({
        groupId: `booking-expiry-test-group-${Date.now()}`,
    });
    await bookingConsumer.connect();
    await bookingConsumer.subscribe({
        topics: ["inventory.events"],
        fromBeginning: false,
    });

    await bookingConsumer.run({
        eachMessage: async ({ message }) => {
            if (!message.value) return;
            const event = JSON.parse(message.value.toString());

            if (event.type === "SeatsReserved" || event.type === "SeatsReservationFailed") {
                const res = await applyInventoryOutcome(event);
                if (res.kind === "updated") relay.trigger();
            } else if (event.type === "HoldExpired") {
                await applyHoldExpired(event);
            }
        },
    });

    // Start Inventory Command Consumer (to handle ReserveSeats)
    const inventoryConsumer = kafka.consumer({
        groupId: `inventory-cmd-test-group-${Date.now()}`,
    });
    await inventoryConsumer.connect();
    await inventoryConsumer.subscribe({
        topic: "inventory.commands",
        fromBeginning: false,
    });

    await inventoryConsumer.run({
        eachMessage: async ({ message }) => {
            if (!message.value) return;
            const cmd = JSON.parse(message.value.toString());

            if (cmd.type === "ReserveSeats") {
                const res = await reserveSeats(
                    cmd.bookingId,
                    cmd.payload.eventId,
                    cmd.payload.seatNos,
                );
                await producer.send({
                    topic: "inventory.events",
                    messages: [
                        {
                            key: cmd.bookingId,
                            value: JSON.stringify({
                                messageId: randomUUID(),
                                bookingId: cmd.bookingId,
                                type: res.success ? "SeatsReserved" : "SeatsReservationFailed",
                                occurredAt: new Date().toISOString(),
                                payload: {
                                    eventId: cmd.payload.eventId,
                                    seatNos: cmd.payload.seatNos,
                                    ...(res.success ? {} : { reason: res.reason }),
                                },
                            }),
                        },
                    ],
                });
            }
        },
    });

    // Start Expiry Sweeper
    const sweeper = new ExpirySweeper(inventoryPool, producer, { sweepIntervalMs: 500 });
    sweeper.start();

    // Allow consumer groups to join
    await new Promise((r) => setTimeout(r, 2000));

    try {
        const testSeats = ["X1", "X2"];
        await setupSeats(testSeats);

        // 1. Create booking
        const createRes = await fetch(`${baseUrl}/bookings`, {
            method: "POST",
            headers: {
                "Content-Type": "application/json",
                "Idempotency-Key": `exp-${Date.now()}`,
            },
            body: JSON.stringify({
                userId: "user-expiry",
                eventId,
                seatNos: testSeats,
            }),
        });

        assert.equal(createRes.status, 202);
        const { bookingId } = (await createRes.json()) as { bookingId: string };

        // 2. Wait for booking to reach SEATS_HELD
        await waitFor(async () => {
            const b = await getBookingById(bookingId);
            return b?.status === "SEATS_HELD";
        });
        console.log("✓ Booking reached SEATS_HELD");

        const heldSeat = await getSeat("X1");
        assert.equal(heldSeat.status, "HELD");
        assert.equal(heldSeat.held_by_booking, bookingId);

        // 3. Fast-forward hold expiry in PostgreSQL
        await inventoryPool.query(
            "UPDATE seats SET hold_expires_at = NOW() - INTERVAL '1 second' WHERE event_id = $1 AND seat_no = ANY($2::text[])",
            [eventId, testSeats],
        );

        // Trigger sweeper immediately
        sweeper.trigger();

        // 4. Wait for Booking status to transition to EXPIRED
        await waitFor(async () => {
            const b = await getBookingById(bookingId);
            return b?.status === "EXPIRED";
        });
        console.log("✓ HoldExpired consumed by Booking; status transitioned to EXPIRED");

        // 5. Verify seats in Inventory are back in AVAILABLE status
        const releasedX1 = await getSeat("X1");
        const releasedX2 = await getSeat("X2");
        assert.equal(releasedX1.status, "AVAILABLE");
        assert.equal(releasedX1.held_by_booking, null);
        assert.equal(releasedX2.status, "AVAILABLE");
        assert.equal(releasedX2.held_by_booking, null);
        console.log("✓ Unpaid seats released back to AVAILABLE in inventory DB");

        // 6. Verify Saga Log records EXPIRED step
        const saga = await getSagaLog(bookingPool, bookingId);
        const states = saga.map((s) => s.state);
        assert.deepEqual(states, ["PENDING", "SEATS_HELD", "EXPIRED"]);
        assert.equal(saga[saga.length - 1].lastEvent, "HoldExpired");
        console.log("✓ Saga log verified:", states);

        // 7. Verify paying an EXPIRED booking is rejected with 409 Conflict
        const payRes = await fetch(`${baseUrl}/bookings/${bookingId}/pay`, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ amount: 100 }),
        });
        assert.equal(payRes.status, 409, "Payment on EXPIRED booking must return 409 Conflict");
        const payError = (await payRes.json()) as { error: string };
        assert.match(payError.error, /Cannot perform payment on booking in state EXPIRED; expected SEATS_HELD/);
        console.log("✓ Payment attempt on EXPIRED booking safely rejected with 409 Conflict");

        // 8. Verify the released seats can immediately be reserved by another user
        const newBookingRes = await fetch(`${baseUrl}/bookings`, {
            method: "POST",
            headers: {
                "Content-Type": "application/json",
                "Idempotency-Key": `re-reserve-${Date.now()}`,
            },
            body: JSON.stringify({
                userId: "user-new",
                eventId,
                seatNos: ["X1"],
            }),
        });
        assert.equal(newBookingRes.status, 202);
        const { bookingId: newBookingId } = (await newBookingRes.json()) as { bookingId: string };

        await waitFor(async () => {
            const b = await getBookingById(newBookingId);
            return b?.status === "SEATS_HELD";
        });
        const reHeldSeat = await getSeat("X1");
        assert.equal(reHeldSeat.status, "HELD");
        assert.equal(reHeldSeat.held_by_booking, newBookingId);
        console.log("✓ Previously expired seat successfully reserved by new booking");

    } finally {
        await sweeper.stop();
        await relay.stop();
        server.close();
        await bookingConsumer.disconnect();
        await inventoryConsumer.disconnect();
        await producer.disconnect();
    }
}

async function main() {
    try {
        await testSweeperUnitAndKafkaPublication();
        await testEndToEndHoldExpiryWorkflow();

        console.log("\n========================================================");
        console.log("ALL PHASE 8 HOLD EXPIRY TESTS PASSED!");
        console.log("========================================================\n");
        process.exit(0);
    } catch (error) {
        console.error("\n❌ Hold expiry test failed:", error);
        process.exit(1);
    } finally {
        await inventoryPool.end();
        await bookingPool.end();
    }
}

main().catch((err) => {
    console.error("Fatal test error:", err);
    process.exit(1);
});
