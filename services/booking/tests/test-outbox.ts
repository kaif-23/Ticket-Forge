import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import { Kafka } from "kafkajs";
import { pool } from "../src/db.js";
import {
    createOrGetBooking,
    IdempotencyConflictError,
} from "../src/booking.js";
import {
    getOutboxRecordById,
    getOutboxRecordsByBookingId,
} from "../src/outbox.js";
import {
    type KafkaProducerLike,
    OutboxRelay,
    publishOutboxBatch,
} from "../src/outboxRelay.js";
import type { ReserveSeatsCommand } from "../../../packages/common/src/messages.js";

const testPrefix = `test-outbox-${Date.now()}`;

class MockProducer implements KafkaProducerLike {
    public sent: Array<{ topic: string; messages: Array<{ key?: string; value: string | Buffer | null }> }> = [];
    public shouldFail = false;
    public failMessage = "simulated Kafka failure";

    async send(record: { topic: string; messages: Array<{ key?: string; value: string | Buffer | null }> }) {
        if (this.shouldFail) {
            throw new Error(this.failMessage);
        }
        this.sent.push(record);
        return [{ topicName: record.topic, partition: 0, errorCode: 0, offset: "0" }];
    }
}

async function cleanupTestData() {
    await pool.query("DELETE FROM bookings WHERE event_id LIKE $1", [`${testPrefix}%`]).catch(() => undefined);
}

async function runAtomicPersistenceTests() {
    console.log("--- Testing Atomic Persistence ---");

    // 1. A new Booking creates exactly one corresponding outbox record atomically.
    const req1 = {
        userId: "user-p1",
        eventId: `${testPrefix}-atomic-1`,
        seatNos: ["A1", "A2"],
    };
    const result1 = await createOrGetBooking(req1, "key-atomic-1");
    assert.equal(result1.created, true);
    assert.ok(result1.booking.id);
    assert.equal(result1.booking.status, "PENDING");
    assert.ok(result1.outboxRecord);
    assert.equal(result1.outboxRecord.bookingId, result1.booking.id);
    assert.equal(result1.outboxRecord.type, "ReserveSeats");
    assert.equal(result1.outboxRecord.topic, "inventory.commands");
    assert.deepEqual(result1.outboxRecord.payload, {
        eventId: `${testPrefix}-atomic-1`,
        seatNos: ["A1", "A2"],
    });

    const outboxRows1 = await getOutboxRecordsByBookingId(pool, result1.booking.id);
    assert.equal(outboxRows1.length, 1);
    assert.equal(outboxRows1[0].id, result1.outboxRecord.id);
    assert.equal(outboxRows1[0].publishedAt, null);
    console.log("✓ Scenario 1: New Booking creates exactly one corresponding outbox record");

    // 2. If outbox insertion fails, the Booking insert rolls back.
    const req2 = {
        userId: "user-p2",
        eventId: `${testPrefix}-atomic-2`,
        seatNos: ["B1"],
    };
    await assert.rejects(
        () =>
            createOrGetBooking(req2, "key-atomic-2", {
                outboxInserter: async () => {
                    throw new Error("Simulated Outbox DB Failure");
                },
            }),
        /Simulated Outbox DB Failure/,
    );

    const bookingCheck = await pool.query(
        "SELECT id FROM bookings WHERE user_id = $1 AND idempotency_key = $2",
        ["user-p2", "key-atomic-2"],
    );
    assert.equal(bookingCheck.rows.length, 0, "Booking must have rolled back");

    const outboxCheck = await pool.query(
        "SELECT id FROM outbox WHERE payload->>'eventId' = $1",
        [`${testPrefix}-atomic-2`],
    );
    assert.equal(outboxCheck.rows.length, 0, "Outbox record must not exist");
    console.log("✓ Scenario 2: If outbox insertion fails, the Booking insert rolls back");

    // 3. If the transaction fails before commit, neither record remains committed.
    const req3 = {
        userId: "user-p3",
        eventId: `${testPrefix}-atomic-3`,
        seatNos: ["C1"],
    };
    const client = await pool.connect();
    try {
        await client.query("BEGIN");
        const bId = randomUUID();
        await client.query(
            `INSERT INTO bookings (id, user_id, event_id, seat_nos, status, idempotency_key, request_hash)
             VALUES ($1, $2, $3, $4::jsonb, 'PENDING', $5, 'hash3')`,
            [bId, req3.userId, req3.eventId, JSON.stringify(req3.seatNos), "key-atomic-3"],
        );
        await client.query(
            `INSERT INTO outbox (id, booking_id, topic, type, occurred_at, payload)
             VALUES ($1, $2, 'inventory.commands', 'ReserveSeats', NOW(), $3::jsonb)`,
            [randomUUID(), bId, JSON.stringify({ eventId: req3.eventId, seatNos: req3.seatNos })],
        );
        // Abort / roll back before commit
        await client.query("ROLLBACK");

        const uncommittedBooking = await pool.query("SELECT id FROM bookings WHERE id = $1", [bId]);
        assert.equal(uncommittedBooking.rows.length, 0);
        const uncommittedOutbox = await pool.query("SELECT id FROM outbox WHERE booking_id = $1", [bId]);
        assert.equal(uncommittedOutbox.rows.length, 0);
    } finally {
        client.release();
    }
    console.log("✓ Scenario 3: If transaction fails before commit, neither record remains committed");

    // 4. An idempotent replay does not create another Booking or outbox record.
    const replayResult = await createOrGetBooking(req1, "key-atomic-1");
    assert.equal(replayResult.created, false);
    assert.equal(replayResult.booking.id, result1.booking.id);
    const replayOutboxRows = await getOutboxRecordsByBookingId(pool, result1.booking.id);
    assert.equal(replayOutboxRows.length, 1, "Must still be exactly 1 outbox record");
    console.log("✓ Scenario 4: Idempotent replay does not create another Booking or outbox record");

    // 5. Conflicting idempotency-key reuse remains a conflict.
    const conflictingReq = {
        userId: "user-p1",
        eventId: `${testPrefix}-atomic-1`,
        seatNos: ["DIFFERENT"],
    };
    await assert.rejects(
        () => createOrGetBooking(conflictingReq, "key-atomic-1"),
        (err) => err instanceof IdempotencyConflictError,
    );
    const afterConflictOutbox = await getOutboxRecordsByBookingId(pool, result1.booking.id);
    assert.equal(afterConflictOutbox.length, 1);
    console.log("✓ Scenario 5: Conflicting idempotency-key reuse remains a conflict");
}

async function runRelayBehaviorTests() {
    console.log("\n--- Testing Relay Behavior ---");

    // 6. A pending outbox message is published to the expected Kafka topic with the exact persisted envelope.
    const req6 = {
        userId: "user-r6",
        eventId: `${testPrefix}-relay-6`,
        seatNos: ["R1", "R2"],
    };
    const b6 = await createOrGetBooking(req6, "key-relay-6");
    const mockProducer = new MockProducer();

    const count6 = await publishOutboxBatch(pool, mockProducer, 10);
    assert.ok(count6 >= 1);

    const sentCommandRecord = mockProducer.sent.find((s) => s.topic === "inventory.commands");
    assert.ok(sentCommandRecord, "Must publish to inventory.commands");
    const sentMsg = sentCommandRecord.messages.find((m) => m.key === b6.booking.id);
    assert.ok(sentMsg, "Message key must be the bookingId");

    const parsedEnvelope = JSON.parse(sentMsg.value as string) as ReserveSeatsCommand;
    assert.equal(parsedEnvelope.type, "ReserveSeats");
    assert.equal(parsedEnvelope.messageId, b6.outboxRecord!.id);
    assert.equal(parsedEnvelope.bookingId, b6.booking.id);
    assert.equal(parsedEnvelope.occurredAt, b6.outboxRecord!.occurredAt);
    assert.deepEqual(parsedEnvelope.payload, {
        eventId: `${testPrefix}-relay-6`,
        seatNos: ["R1", "R2"],
    });
    console.log("✓ Scenario 6: Pending outbox message published with exact persisted envelope");

    // 7. A successful Kafka acknowledgement is followed by marking the record published.
    const record6After = await getOutboxRecordById(pool, b6.outboxRecord!.id);
    assert.ok(record6After);
    assert.ok(record6After.publishedAt !== null, "publishedAt must be set");
    assert.ok(record6After.publishedAt instanceof Date);
    console.log("✓ Scenario 7: Successful Kafka ack marks outbox record published");

    // 8. A Kafka publication failure leaves the record retryable.
    const req8 = {
        userId: "user-r8",
        eventId: `${testPrefix}-relay-8`,
        seatNos: ["S1"],
    };
    const b8 = await createOrGetBooking(req8, "key-relay-8");
    const failingProducer = new MockProducer();
    failingProducer.shouldFail = true;

    await assert.rejects(
        () => publishOutboxBatch(pool, failingProducer, 10),
        /simulated Kafka failure/,
    );

    const record8Before = await getOutboxRecordById(pool, b8.outboxRecord!.id);
    assert.ok(record8Before);
    assert.equal(record8Before.publishedAt, null, "Record must remain unpublished after Kafka failure");

    // Retry with healthy producer
    const healthyProducer = new MockProducer();
    const retryCount = await publishOutboxBatch(pool, healthyProducer, 10);
    assert.ok(retryCount >= 1);

    const record8After = await getOutboxRecordById(pool, b8.outboxRecord!.id);
    assert.ok(record8After);
    assert.ok(record8After.publishedAt !== null, "Record must be marked published after retry succeeds");
    console.log("✓ Scenario 8: Kafka publication failure leaves record retryable");

    // 9. A process/relay restart can publish a previously pending record.
    const req9 = {
        userId: "user-r9",
        eventId: `${testPrefix}-relay-9`,
        seatNos: ["T1"],
    };
    const b9 = await createOrGetBooking(req9, "key-relay-9");

    // Simulate relay 1 stopped before publishing
    const producer9 = new MockProducer();
    const relay1 = new OutboxRelay(pool, producer9, { pollIntervalMs: 5000 });
    // Not calling relay1.start() - simulating process death before relay ticks

    // Restart: instantiate and run relay2
    const relay2 = new OutboxRelay(pool, producer9, { pollIntervalMs: 5000 });
    const publishedByRelay2 = await relay2.pollOnce();
    assert.ok(publishedByRelay2 >= 1);

    const record9After = await getOutboxRecordById(pool, b9.outboxRecord!.id);
    assert.ok(record9After);
    assert.ok(record9After.publishedAt !== null, "Previously pending record published after restart");
    console.log("✓ Scenario 9: Process/relay restart publishes previously pending record");

    // 10. Two relay workers do not concurrently claim the same record in a way that causes unsafe state updates.
    const req10 = {
        userId: "user-r10",
        eventId: `${testPrefix}-relay-10`,
        seatNos: ["U1"],
    };
    const b10 = await createOrGetBooking(req10, "key-relay-10");

    const clientWorker1 = await pool.connect();
    const clientWorker2 = await pool.connect();

    try {
        await clientWorker1.query("BEGIN");
        const w1Select = await clientWorker1.query(
            `SELECT id FROM outbox WHERE id = $1 AND published_at IS NULL FOR UPDATE SKIP LOCKED`,
            [b10.outboxRecord!.id],
        );
        assert.equal(w1Select.rows.length, 1, "Worker 1 must claim the row");

        // Worker 2 attempts to claim the same row concurrently
        await clientWorker2.query("BEGIN");
        const w2Select = await clientWorker2.query(
            `SELECT id FROM outbox WHERE id = $1 AND published_at IS NULL FOR UPDATE SKIP LOCKED`,
            [b10.outboxRecord!.id],
        );
        assert.equal(w2Select.rows.length, 0, "Worker 2 must skip the locked row (0 rows claimed)");

        // Worker 1 completes publish and marks published
        await clientWorker1.query("UPDATE outbox SET published_at = NOW() WHERE id = $1", [b10.outboxRecord!.id]);
        await clientWorker1.query("COMMIT");

        // Worker 2 commits its empty transaction
        await clientWorker2.query("COMMIT");

        const record10Final = await getOutboxRecordById(pool, b10.outboxRecord!.id);
        assert.ok(record10Final!.publishedAt !== null);
    } finally {
        clientWorker1.release();
        clientWorker2.release();
    }
    console.log("✓ Scenario 10: Two relay workers do not concurrently claim the same record (FOR UPDATE SKIP LOCKED)");

    // 11. A crash after Kafka acknowledgement but before published status update causes redelivery with stable message ID.
    const req11 = {
        userId: "user-r11",
        eventId: `${testPrefix}-relay-11`,
        seatNos: ["V1"],
    };
    const b11 = await createOrGetBooking(req11, "key-relay-11");
    const stableMessageId = b11.outboxRecord!.id;

    // Simulate crash after Kafka send: producer records message, but DB transaction rolls back before status update
    const firstAttemptProducer = new MockProducer();
    const crashClient = await pool.connect();
    try {
        await crashClient.query("BEGIN");
        const rowRes = await crashClient.query(
            `SELECT id, booking_id, topic, type, occurred_at, payload FROM outbox WHERE id = $1 FOR UPDATE`,
            [stableMessageId],
        );
        assert.equal(rowRes.rows.length, 1);
        const row = rowRes.rows[0];

        // Send to Kafka
        await firstAttemptProducer.send({
            topic: row.topic,
            messages: [{
                key: row.booking_id,
                value: JSON.stringify({
                    messageId: row.id,
                    bookingId: row.booking_id,
                    type: row.type,
                    occurredAt: row.occurred_at.toISOString(),
                    payload: row.payload,
                }),
            }],
        });

        // Simulate crash right before UPDATE / COMMIT
        await crashClient.query("ROLLBACK");
    } finally {
        crashClient.release();
    }

    assert.equal(firstAttemptProducer.sent.length, 1);
    const firstPublishedMessage = JSON.parse(firstAttemptProducer.sent[0].messages[0].value as string);
    assert.equal(firstPublishedMessage.messageId, stableMessageId);

    // Verify DB still says unpublished
    const rowAfterCrash = await getOutboxRecordById(pool, stableMessageId);
    assert.equal(rowAfterCrash!.publishedAt, null);

    // After restart: second relay publishes again
    const secondAttemptProducer = new MockProducer();
    await publishOutboxBatch(pool, secondAttemptProducer, 10);

    const secondSentRecord = secondAttemptProducer.sent.find((s) => s.topic === "inventory.commands");
    assert.ok(secondSentRecord);
    const secondSentMsg = secondSentRecord.messages.find((m) => m.key === b11.booking.id);
    assert.ok(secondSentMsg);
    const secondPublishedMessage = JSON.parse(secondSentMsg.value as string);

    // Crucial invariant: identical messageId preserved across redeliveries!
    assert.equal(
        secondPublishedMessage.messageId,
        stableMessageId,
        "Message ID must be preserved identically across attempts",
    );

    const rowFinal = await getOutboxRecordById(pool, stableMessageId);
    assert.ok(rowFinal!.publishedAt !== null, "Outbox record marked published after redelivery");
    console.log("✓ Scenario 11: Crash after Kafka ack but before published status update causes redelivery with stable message ID");

    // 12. Relay background poller lifecycle, trigger, and clean shutdown
    const req12 = {
        userId: "user-r12",
        eventId: `${testPrefix}-relay-12`,
        seatNos: ["W1"],
    };
    const b12 = await createOrGetBooking(req12, "key-relay-12");
    const bgProducer = new MockProducer();
    const bgRelay = new OutboxRelay(pool, bgProducer, { pollIntervalMs: 100 });
    bgRelay.start();
    bgRelay.trigger();

    const pollStart = Date.now();
    let record12 = await getOutboxRecordById(pool, b12.outboxRecord!.id);
    while (record12?.publishedAt === null && Date.now() - pollStart < 5000) {
        await new Promise((r) => setTimeout(r, 50));
        record12 = await getOutboxRecordById(pool, b12.outboxRecord!.id);
    }
    assert.ok(record12?.publishedAt !== null, "Background poller must publish and mark record published");
    await bgRelay.stop();
    console.log("✓ Scenario 12: Background poller lifecycle, trigger, and clean shutdown verified");
}

async function runRealKafkaIntegrationTest() {
    console.log("\n--- Testing Real Kafka & PostgreSQL Integration ---");

    const kafka = new Kafka({
        clientId: "test-outbox-real-client",
        brokers: ["localhost:9092"],
    });

    const realProducer = kafka.producer();
    const realConsumer = kafka.consumer({
        groupId: `test-outbox-group-${Date.now()}`,
    });

    try {
        await realProducer.connect();
        await realConsumer.connect();
        await realConsumer.subscribe({
            topic: "inventory.commands",
            fromBeginning: true,
        });

        // Create booking with outbox record
        const req = {
            userId: "user-real-kafka",
            eventId: `${testPrefix}-kafka-real`,
            seatNos: ["K1", "K2"],
        };
        const bookingResult = await createOrGetBooking(req, `key-real-${Date.now()}`);
        const expectedMessageId = bookingResult.outboxRecord!.id;
        const expectedBookingId = bookingResult.booking.id;

        // Set up consumer message listener
        const receivedMessages: ReserveSeatsCommand[] = [];
        await realConsumer.run({
            eachMessage: async ({ message }) => {
                if (!message.value) return;
                try {
                    const parsed = JSON.parse(message.value.toString()) as ReserveSeatsCommand;
                    if (parsed.bookingId === expectedBookingId) {
                        receivedMessages.push(parsed);
                    }
                } catch {
                    // ignore other messages
                }
            },
        });

        // Allow consumer group to complete initial join and partition assignment
        await new Promise((r) => setTimeout(r, 1500));

        // Run relay to publish to real Kafka
        const relay = new OutboxRelay(pool, realProducer, { pollIntervalMs: 500 });
        await relay.pollOnce();

        // Wait for real Kafka consumer to receive message
        const start = Date.now();
        while (receivedMessages.length === 0 && Date.now() - start < 10000) {
            await new Promise((r) => setTimeout(r, 200));
        }

        assert.equal(receivedMessages.length, 1, "Real Kafka consumer must receive the message");
        const msg = receivedMessages[0];
        assert.equal(msg.type, "ReserveSeats");
        assert.equal(msg.messageId, expectedMessageId);
        assert.equal(msg.bookingId, expectedBookingId);
        assert.deepEqual(msg.payload, {
            eventId: `${testPrefix}-kafka-real`,
            seatNos: ["K1", "K2"],
        });

        // Verify outbox record in DB marked published
        const outboxRecord = await getOutboxRecordById(pool, expectedMessageId);
        assert.ok(outboxRecord);
        assert.ok(outboxRecord.publishedAt !== null, "Outbox record must be marked published in PostgreSQL");

        console.log("✓ Real Integration: Booking → Outbox → Real Kafka broker → Consumer verified!");
    } finally {
        await realConsumer.disconnect().catch(() => undefined);
        await realProducer.disconnect().catch(() => undefined);
    }
}

async function main() {
    try {
        await cleanupTestData();
        await runAtomicPersistenceTests();
        await runRelayBehaviorTests();
        await runRealKafkaIntegrationTest();
        console.log("\n=========================================");
        console.log("All 11 Transactional Outbox Scenarios and Real Kafka Integration PASSED!");
        console.log("=========================================");
    } finally {
        await cleanupTestData();
        await pool.end().catch(() => undefined);
    }
}

main().catch((error) => {
    console.error("Outbox verification failed:", error);
    process.exitCode = 1;
});
