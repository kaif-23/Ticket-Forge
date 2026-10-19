import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { pool as bookingPool } from "../src/db.js";
import { pool as inventoryPool } from "../../inventory/src/db.js";
import { pool as paymentPool } from "../../payment/src/db.js";
import { createOrGetBooking, getBookingById } from "../src/booking.js";
import {
    applyHoldExpired,
    applyInventoryOutcome,
    applyPaymentOutcome,
    applySeatsConfirmed,
} from "../src/bookingEvents.js";
import { reserveSeats } from "../../inventory/src/reserveSeats.js";
import { confirmSeats } from "../../inventory/src/confirmSeats.js";
import { releaseSeats } from "../../inventory/src/releaseSeats.js";
import { recordProcessedMessage as recordInventoryProcessedMessage } from "../../inventory/src/idempotency.js";
import { closeRedis } from "../../inventory/src/redis.js";
import { executePayment, getPaymentByBookingId } from "../../payment/src/payments.js";

const testRunId = `idemp-${Date.now()}`;
const eventId = `event-${testRunId}`;

async function setupInventorySeats(seatNos: string[]) {
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
        version: number;
    }>(
        "SELECT status, held_by_booking, version FROM seats WHERE event_id = $1 AND seat_no = $2",
        [eventId, seatNo],
    );
    return res.rows[0];
}

async function getBookingOutboxCount(bookingId: string): Promise<number> {
    const res = await bookingPool.query<{ count: string }>(
        "SELECT COUNT(*) AS count FROM outbox WHERE booking_id = $1",
        [bookingId],
    );
    return Number(res.rows[0].count);
}

async function getBookingSagaLogCount(bookingId: string): Promise<number> {
    const res = await bookingPool.query<{ count: string }>(
        "SELECT COUNT(*) AS count FROM saga_log WHERE booking_id = $1",
        [bookingId],
    );
    return Number(res.rows[0].count);
}

async function getBookingProcessedMessagesCount(messageId: string): Promise<number> {
    const res = await bookingPool.query<{ count: string }>(
        "SELECT COUNT(*) AS count FROM processed_messages WHERE message_id = $1",
        [messageId],
    );
    return Number(res.rows[0].count);
}

// =========================================================================
// TEST SUITE 1: Booking Service Event Deduplication & Redelivery
// =========================================================================
async function testBookingEventDeduplication() {
    console.log("\n==========================================================");
    console.log("1. Testing Booking Service Idempotency & Redelivery");
    console.log("==========================================================");

    // ---------------------------------------------------------------------
    // 1.1 Duplicate SeatsReserved Event
    // ---------------------------------------------------------------------
    const { booking: booking1 } = await createOrGetBooking(
        {
            userId: "user-idemp-1",
            eventId,
            seatNos: ["A1"],
        },
        `key-1-${testRunId}`,
    );
    assert.equal(booking1.status, "PENDING");

    const reservedMsgId = randomUUID();
    const seatsReservedEvent = {
        messageId: reservedMsgId,
        bookingId: booking1.id,
        type: "SeatsReserved",
        occurredAt: new Date().toISOString(),
        payload: {
            eventId,
            seatNos: ["A1"],
        },
    };

    // First delivery
    const resReserved1 = await applyInventoryOutcome(seatsReservedEvent);
    assert.equal(resReserved1.kind, "updated");
    assert.equal(resReserved1.status, "SEATS_HELD");

    const updatedBooking1 = await getBookingById(booking1.id);
    assert.equal(updatedBooking1?.status, "SEATS_HELD");

    const processedCount1 = await getBookingProcessedMessagesCount(reservedMsgId);
    assert.equal(processedCount1, 1, "processed_messages should have 1 entry");

    const sagaLogCount1 = await getBookingSagaLogCount(booking1.id);
    assert.equal(sagaLogCount1, 2, "saga_log should have 2 entries (BookingCreated + SeatsReserved)");

    // Second delivery (Duplicate redelivery of same Kafka message)
    const resReserved2 = await applyInventoryOutcome(seatsReservedEvent);
    assert.equal(resReserved2.kind, "duplicate");
    assert.equal(resReserved2.messageId, reservedMsgId);
    assert.equal(resReserved2.bookingId, booking1.id);

    // Verify ZERO side effects
    const bookingAfterDup = await getBookingById(booking1.id);
    assert.equal(bookingAfterDup?.status, "SEATS_HELD", "Booking status must not change");

    const processedCountAfterDup = await getBookingProcessedMessagesCount(reservedMsgId);
    assert.equal(processedCountAfterDup, 1, "No duplicate processed_messages row");

    const sagaLogCountAfterDup = await getBookingSagaLogCount(booking1.id);
    assert.equal(sagaLogCountAfterDup, 2, "No duplicate saga_log entries");
    console.log("✓ Duplicate SeatsReserved redelivery detected and produced zero side-effects");

    // ---------------------------------------------------------------------
    // 1.2 Duplicate PaymentSucceeded Event
    // ---------------------------------------------------------------------
    const paymentSucceededMsgId = randomUUID();
    const paymentSucceededEvent = {
        messageId: paymentSucceededMsgId,
        bookingId: booking1.id,
        type: "PaymentSucceeded",
        occurredAt: new Date().toISOString(),
        payload: {
            paymentId: randomUUID(),
            amount: 150,
        },
    };

    // Initial outbox count
    const outboxCountBefore = await getBookingOutboxCount(booking1.id);

    // First delivery
    const resPay1 = await applyPaymentOutcome(paymentSucceededEvent);
    assert.equal(resPay1.kind, "updated");
    assert.equal(resPay1.status, "PAID");

    const outboxCountAfterFirst = await getBookingOutboxCount(booking1.id);
    assert.equal(outboxCountAfterFirst, outboxCountBefore + 1, "Should emit ConfirmSeats command to outbox");

    const sagaCountAfterFirstPay = await getBookingSagaLogCount(booking1.id);
    assert.equal(sagaCountAfterFirstPay, 3, "saga_log should record PAID (total 3 entries)");

    // Second delivery (Duplicate Kafka message)
    const resPay2 = await applyPaymentOutcome(paymentSucceededEvent);
    assert.equal(resPay2.kind, "duplicate");
    assert.equal(resPay2.messageId, paymentSucceededMsgId);

    // Verify ZERO duplicate side effects
    const bookingAfterDupPay = await getBookingById(booking1.id);
    assert.equal(bookingAfterDupPay?.status, "PAID");

    const outboxCountAfterDup = await getBookingOutboxCount(booking1.id);
    assert.equal(outboxCountAfterDup, outboxCountAfterFirst, "Zero duplicate outbox rows emitted on duplicate payment event");

    const sagaCountAfterDupPay = await getBookingSagaLogCount(booking1.id);
    assert.equal(sagaCountAfterDupPay, 3, "Zero duplicate saga_log rows");
    console.log("✓ Duplicate PaymentSucceeded redelivery detected: 0 duplicate outbox rows, 0 duplicate saga logs");

    // ---------------------------------------------------------------------
    // 1.3 Duplicate SeatsConfirmed Event
    // ---------------------------------------------------------------------
    const seatsConfirmedMsgId = randomUUID();
    const seatsConfirmedEvent = {
        messageId: seatsConfirmedMsgId,
        bookingId: booking1.id,
        type: "SeatsConfirmed",
        occurredAt: new Date().toISOString(),
        payload: {
            eventId,
            seatNos: ["A1"],
        },
    };

    // First delivery
    const resConf1 = await applySeatsConfirmed(seatsConfirmedEvent);
    assert.equal(resConf1.kind, "updated");
    assert.equal(resConf1.status, "CONFIRMED");

    const outboxCountAfterConf1 = await getBookingOutboxCount(booking1.id);
    assert.equal(outboxCountAfterConf1, outboxCountAfterDup + 1, "Should emit BookingConfirmed event in outbox");

    // Second delivery (Duplicate redelivery)
    const resConf2 = await applySeatsConfirmed(seatsConfirmedEvent);
    assert.equal(resConf2.kind, "duplicate");
    assert.equal(resConf2.messageId, seatsConfirmedMsgId);

    const outboxCountAfterConf2 = await getBookingOutboxCount(booking1.id);
    assert.equal(outboxCountAfterConf2, outboxCountAfterConf1, "Zero duplicate outbox rows emitted on duplicate SeatsConfirmed");

    const finalBooking1 = await getBookingById(booking1.id);
    assert.equal(finalBooking1?.status, "CONFIRMED");
    console.log("✓ Duplicate SeatsConfirmed redelivery detected: 0 duplicate outbox rows");

    // ---------------------------------------------------------------------
    // 1.4 Duplicate PaymentFailed Event (Compensation Path Deduplication)
    // ---------------------------------------------------------------------
    const { booking: bookingFail } = await createOrGetBooking(
        {
            userId: "user-idemp-fail",
            eventId,
            seatNos: ["A2"],
        },
        `key-fail-${testRunId}`,
    );
    // Transition to SEATS_HELD
    await applyInventoryOutcome({
        messageId: randomUUID(),
        bookingId: bookingFail.id,
        type: "SeatsReserved",
        occurredAt: new Date().toISOString(),
        payload: { eventId, seatNos: ["A2"] },
    });

    const payFailMsgId = randomUUID();
    const payFailEvent = {
        messageId: payFailMsgId,
        bookingId: bookingFail.id,
        type: "PaymentFailed",
        occurredAt: new Date().toISOString(),
        payload: { reason: "INSUFFICIENT_FUNDS" },
    };

    // First delivery
    const resFail1 = await applyPaymentOutcome(payFailEvent);
    assert.equal(resFail1.kind, "updated");
    assert.equal(resFail1.status, "CANCELLED");

    const failOutboxCount1 = await getBookingOutboxCount(bookingFail.id);
    // Emits ReleaseSeats command + BookingCancelled event = 2 outbox records
    assert.ok(failOutboxCount1 >= 2);

    // Second delivery (Duplicate redelivery)
    const resFail2 = await applyPaymentOutcome(payFailEvent);
    assert.equal(resFail2.kind, "duplicate");
    assert.equal(resFail2.messageId, payFailMsgId);

    const failOutboxCount2 = await getBookingOutboxCount(bookingFail.id);
    assert.equal(failOutboxCount2, failOutboxCount1, "Zero duplicate compensation outbox records on duplicate PaymentFailed");
    console.log("✓ Duplicate PaymentFailed redelivery detected: 0 duplicate compensation outbox records");

    // ---------------------------------------------------------------------
    // 1.5 Duplicate HoldExpired Event
    // ---------------------------------------------------------------------
    const { booking: bookingExp } = await createOrGetBooking(
        {
            userId: "user-idemp-exp",
            eventId,
            seatNos: ["A3"],
        },
        `key-exp-${testRunId}`,
    );
    await applyInventoryOutcome({
        messageId: randomUUID(),
        bookingId: bookingExp.id,
        type: "SeatsReserved",
        occurredAt: new Date().toISOString(),
        payload: { eventId, seatNos: ["A3"] },
    });

    const expMsgId = randomUUID();
    const holdExpiredEvent = {
        messageId: expMsgId,
        bookingId: bookingExp.id,
        type: "HoldExpired",
        occurredAt: new Date().toISOString(),
        payload: { eventId, seatNos: ["A3"] },
    };

    const resExp1 = await applyHoldExpired(holdExpiredEvent);
    assert.equal(resExp1.kind, "updated");
    assert.equal(resExp1.status, "EXPIRED");

    const resExp2 = await applyHoldExpired(holdExpiredEvent);
    assert.equal(resExp2.kind, "duplicate");
    assert.equal(resExp2.messageId, expMsgId);

    const expBooking = await getBookingById(bookingExp.id);
    assert.equal(expBooking?.status, "EXPIRED");
    console.log("✓ Duplicate HoldExpired redelivery detected and safely ignored");
}

// =========================================================================
// TEST SUITE 2: Inventory Service Command Deduplication
// =========================================================================
async function testInventoryCommandDeduplication() {
    console.log("\n==========================================================");
    console.log("2. Testing Inventory Service Idempotency & Redelivery");
    console.log("==========================================================");

    await setupInventorySeats(["B1", "B2"]);

    // ---------------------------------------------------------------------
    // 2.1 Duplicate ReserveSeats Command
    // ---------------------------------------------------------------------
    const bookingId = randomUUID();
    const reserveMsgId = randomUUID();

    // First command execution (simulating consumer transaction)
    const client1 = await inventoryPool.connect();
    try {
        await client1.query("BEGIN");
        const { isDuplicate } = await recordInventoryProcessedMessage(
            client1,
            reserveMsgId,
            "inventory-service-group",
            "inventory.commands",
        );
        assert.equal(isDuplicate, false);

        const res = await reserveSeats(bookingId, eventId, ["B1"], client1);
        assert.equal(res.success, true);
        await client1.query("COMMIT");
    } finally {
        client1.release();
    }

    const seatB1After1 = await getInventorySeat("B1");
    assert.equal(seatB1After1.status, "HELD");
    assert.equal(seatB1After1.held_by_booking, bookingId);
    assert.equal(seatB1After1.version, 1, "Seat version should be incremented to 1");

    // Second command execution (Duplicate redelivery)
    const client2 = await inventoryPool.connect();
    try {
        await client2.query("BEGIN");
        const { isDuplicate } = await recordInventoryProcessedMessage(
            client2,
            reserveMsgId,
            "inventory-service-group",
            "inventory.commands",
        );
        assert.equal(isDuplicate, true, "Must detect duplicate messageId in inventory-service-group");

        await client2.query("ROLLBACK");
    } finally {
        client2.release();
    }

    const seatB1After2 = await getInventorySeat("B1");
    assert.equal(seatB1After2.status, "HELD");
    assert.equal(seatB1After2.version, 1, "Version must remain exactly 1 (0 duplicate version increment)");
    console.log("✓ Duplicate ReserveSeats command detected: 0 state changes, 0 duplicate version increments");

    // ---------------------------------------------------------------------
    // 2.2 Duplicate ConfirmSeats Command
    // ---------------------------------------------------------------------
    const confirmMsgId = randomUUID();
    const clientConf1 = await inventoryPool.connect();
    try {
        await clientConf1.query("BEGIN");
        const { isDuplicate } = await recordInventoryProcessedMessage(
            clientConf1,
            confirmMsgId,
            "inventory-service-group",
            "inventory.commands",
        );
        assert.equal(isDuplicate, false);

        const res = await confirmSeats(bookingId, eventId, ["B1"], clientConf1);
        assert.equal(res.success, true);
        await clientConf1.query("COMMIT");
    } finally {
        clientConf1.release();
    }

    const seatB1Confirmed = await getInventorySeat("B1");
    assert.equal(seatB1Confirmed.status, "SOLD");
    assert.equal(seatB1Confirmed.version, 2);

    // Duplicate ConfirmSeats execution
    const clientConf2 = await inventoryPool.connect();
    try {
        await clientConf2.query("BEGIN");
        const { isDuplicate } = await recordInventoryProcessedMessage(
            clientConf2,
            confirmMsgId,
            "inventory-service-group",
            "inventory.commands",
        );
        assert.equal(isDuplicate, true);
        await clientConf2.query("ROLLBACK");
    } finally {
        clientConf2.release();
    }

    const seatB1ConfirmedAfterDup = await getInventorySeat("B1");
    assert.equal(seatB1ConfirmedAfterDup.status, "SOLD");
    assert.equal(seatB1ConfirmedAfterDup.version, 2, "Version unchanged");
    console.log("✓ Duplicate ConfirmSeats command detected: 0 state changes, 0 version changes");

    // ---------------------------------------------------------------------
    // 2.3 Duplicate ReleaseSeats Command
    // ---------------------------------------------------------------------
    const bookingRelId = randomUUID();
    await reserveSeats(bookingRelId, eventId, ["B2"]);
    const seatB2Held = await getInventorySeat("B2");
    assert.equal(seatB2Held.status, "HELD");

    const releaseMsgId = randomUUID();
    const clientRel1 = await inventoryPool.connect();
    try {
        await clientRel1.query("BEGIN");
        const { isDuplicate } = await recordInventoryProcessedMessage(
            clientRel1,
            releaseMsgId,
            "inventory-service-group",
            "inventory.commands",
        );
        assert.equal(isDuplicate, false);

        const res = await releaseSeats(bookingRelId, eventId, ["B2"], clientRel1);
        assert.equal(res.success, true);
        await clientRel1.query("COMMIT");
    } finally {
        clientRel1.release();
    }

    const seatB2Released = await getInventorySeat("B2");
    assert.equal(seatB2Released.status, "AVAILABLE");
    const vBefore = seatB2Released.version;

    // Duplicate ReleaseSeats execution
    const clientRel2 = await inventoryPool.connect();
    try {
        await clientRel2.query("BEGIN");
        const { isDuplicate } = await recordInventoryProcessedMessage(
            clientRel2,
            releaseMsgId,
            "inventory-service-group",
            "inventory.commands",
        );
        assert.equal(isDuplicate, true);
        await clientRel2.query("ROLLBACK");
    } finally {
        clientRel2.release();
    }

    const seatB2ReleasedAfterDup = await getInventorySeat("B2");
    assert.equal(seatB2ReleasedAfterDup.version, vBefore, "Version unchanged after duplicate ReleaseSeats");
    console.log("✓ Duplicate ReleaseSeats command detected: 0 state changes, 0 version changes");
}

// =========================================================================
// TEST SUITE 3: Payment Service Message Deduplication
// =========================================================================
async function testPaymentServiceDeduplication() {
    console.log("\n==========================================================");
    console.log("3. Testing Payment Service Idempotency & Redelivery");
    console.log("==========================================================");

    const bookingId = randomUUID();
    const msgId = randomUUID();

    // 1. First execution
    const res1 = await executePayment(paymentPool, {
        messageId: msgId,
        bookingId,
        type: "ProcessPayment",
        occurredAt: new Date().toISOString(),
        payload: { amount: 200, currency: "USD" },
    });

    assert.equal(res1.isDuplicate, false);
    assert.equal(res1.payment.status, "SUCCEEDED");
    assert.equal(res1.payment.amount, 200);

    const paymentRows1 = await paymentPool.query(
        "SELECT COUNT(*) AS count FROM payments WHERE booking_id = $1",
        [bookingId],
    );
    assert.equal(Number(paymentRows1.rows[0].count), 1);

    const procRows1 = await paymentPool.query(
        "SELECT COUNT(*) AS count FROM processed_messages WHERE message_id = $1 AND consumer_group = $2",
        [msgId, "payment-service-group"],
    );
    assert.equal(Number(procRows1.rows[0].count), 1);

    // 2. Duplicate redelivery with identical messageId
    const res2 = await executePayment(paymentPool, {
        messageId: msgId,
        bookingId,
        type: "ProcessPayment",
        occurredAt: new Date().toISOString(),
        payload: { amount: 200, currency: "USD" },
    });

    assert.equal(res2.isDuplicate, true, "Duplicate messageId must yield isDuplicate = true");
    assert.equal(res2.payment.id, res1.payment.id, "Must return existing payment record");
    assert.equal(res2.payment.status, "SUCCEEDED");

    const paymentRows2 = await paymentPool.query(
        "SELECT COUNT(*) AS count FROM payments WHERE booking_id = $1",
        [bookingId],
    );
    assert.equal(Number(paymentRows2.rows[0].count), 1, "0 duplicate rows in payments table");

    const procRows2 = await paymentPool.query(
        "SELECT COUNT(*) AS count FROM processed_messages WHERE message_id = $1 AND consumer_group = $2",
        [msgId, "payment-service-group"],
    );
    assert.equal(Number(procRows2.rows[0].count), 1, "0 duplicate rows in processed_messages table");
    console.log("✓ Duplicate ProcessPayment command detected: 0 duplicate payments, 0 duplicate DB records");
}

// =========================================================================
// TEST SUITE 4: Consumer Group Independence
// =========================================================================
async function testConsumerGroupIndependence() {
    console.log("\n==========================================================");
    console.log("4. Testing Multi-Consumer Group Independence");
    console.log("==========================================================");

    const broadcastMsgId = randomUUID();

    // Consumer Group A
    const resA1 = await recordInventoryProcessedMessage(
        inventoryPool,
        broadcastMsgId,
        "group-a",
        "events.broadcast",
    );
    assert.equal(resA1.isDuplicate, false);

    // Consumer Group B processes same messageId
    const resB1 = await recordInventoryProcessedMessage(
        inventoryPool,
        broadcastMsgId,
        "group-b",
        "events.broadcast",
    );
    assert.equal(resB1.isDuplicate, false, "Different consumer group must be able to process same message");

    // Duplicate to Consumer Group A
    const resA2 = await recordInventoryProcessedMessage(
        inventoryPool,
        broadcastMsgId,
        "group-a",
        "events.broadcast",
    );
    assert.equal(resA2.isDuplicate, true, "Same consumer group must detect duplicate");

    console.log("✓ Multiple consumer groups process shared broadcast message independently with per-group deduplication");
}

async function main() {
    try {
        await testBookingEventDeduplication();
        await testInventoryCommandDeduplication();
        await testPaymentServiceDeduplication();
        await testConsumerGroupIndependence();

        console.log("\n==========================================================");
        console.log("ALL PHASE 9 IDEMPOTENCY & REDELIVERY TESTS PASSED!");
        console.log("==========================================================");
    } finally {
        await closeRedis();
        await bookingPool.end();
        await inventoryPool.end();
        await paymentPool.end();
    }
}

main().catch((err) => {
    console.error("Test failed:", err);
    process.exit(1);
});
