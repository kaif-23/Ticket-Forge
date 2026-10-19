import assert from "node:assert/strict";
import type { AddressInfo } from "node:net";
import { createBookingApp } from "../src/bookingApi.js";
import { pool } from "../src/db.js";

const eventPrefix = `test-booking-${Date.now()}`;
const triggeredBookingIds: string[] = [];

const app = createBookingApp(async (bookingId) => {
    triggeredBookingIds.push(bookingId);
});

async function main() {
    const server = await new Promise<ReturnType<typeof app.listen>>((resolve) => {
        const listeningServer = app.listen(0, () => resolve(listeningServer));
    });
    const port = (server.address() as AddressInfo).port;
    const baseUrl = `http://127.0.0.1:${port}`;
    let poolClosed = false;

    async function postBooking(
        body: unknown,
        idempotencyKey?: string,
    ): Promise<{ response: Response; body: Record<string, unknown> }> {
        const response = await fetch(`${baseUrl}/bookings`, {
            method: "POST",
            headers: {
                "Content-Type": "application/json",
                ...(idempotencyKey ? { "Idempotency-Key": idempotencyKey } : {}),
            },
            body: JSON.stringify(body),
        });
        return { response, body: (await response.json()) as Record<string, unknown> };
    }

    try {
        const first = await postBooking(
            { userId: "user-1", eventId: `${eventPrefix}-one`, seatNos: ["A2", "A1"] },
            "key-1",
        );
        assert.equal(first.response.status, 202);
        assert.equal(first.body.status, "PENDING");
        assert.equal(typeof first.body.bookingId, "string");
        assert.equal(triggeredBookingIds.length, 1);
        assert.equal(triggeredBookingIds[0], first.body.bookingId);

        const persisted = await pool.query(
            `SELECT id, user_id, event_id, seat_nos, status, idempotency_key, request_hash
             FROM bookings WHERE id = $1`,
            [first.body.bookingId],
        );
        assert.equal(persisted.rows[0].status, "PENDING");
        assert.deepEqual(persisted.rows[0].seat_nos, ["A1", "A2"]);
        assert.ok(persisted.rows[0].request_hash);

        // Verify outbox record created in the same database transaction
        const outboxResult = await pool.query(
            `SELECT id, booking_id, topic, type, occurred_at, payload, published_at
             FROM outbox WHERE booking_id = $1`,
            [first.body.bookingId],
        );
        assert.equal(outboxResult.rows.length, 1);
        const outboxRow = outboxResult.rows[0];
        assert.equal(outboxRow.booking_id, first.body.bookingId);
        assert.equal(outboxRow.type, "ReserveSeats");
        assert.equal(outboxRow.topic, "inventory.commands");
        assert.equal(typeof outboxRow.id, "string");
        assert.ok(outboxRow.occurred_at);
        assert.deepEqual(outboxRow.payload, {
            eventId: `${eventPrefix}-one`,
            seatNos: ["A1", "A2"],
        });
        assert.equal(outboxRow.published_at, null);

        // Idempotent replay
        const replay = await postBooking(
            { userId: "user-1", eventId: `${eventPrefix}-one`, seatNos: ["A1", "A2"] },
            "key-1",
        );
        assert.equal(replay.response.status, 200);
        assert.deepEqual(replay.body, {
            bookingId: first.body.bookingId,
            status: "PENDING",
        });
        // Outbox must not have a second record
        const replayOutbox = await pool.query(
            `SELECT id FROM outbox WHERE booking_id = $1`,
            [first.body.bookingId],
        );
        assert.equal(replayOutbox.rows.length, 1);

        // Idempotency conflict
        const conflict = await postBooking(
            { userId: "user-1", eventId: `${eventPrefix}-different`, seatNos: ["A1"] },
            "key-1",
        );
        assert.equal(conflict.response.status, 409);

        // Different user with same idempotency key succeeds
        const differentUser = await postBooking(
            { userId: "user-2", eventId: `${eventPrefix}-one`, seatNos: ["A1"] },
            "key-1",
        );
        assert.equal(differentUser.response.status, 202);
        assert.notEqual(differentUser.body.bookingId, first.body.bookingId);

        const differentUserOutbox = await pool.query(
            `SELECT id FROM outbox WHERE booking_id = $1`,
            [differentUser.body.bookingId],
        );
        assert.equal(differentUserOutbox.rows.length, 1);

        // Validation errors
        const missingKey = await postBooking({
            userId: "user-3",
            eventId: `${eventPrefix}-invalid`,
            seatNos: ["A1"],
        });
        assert.equal(missingKey.response.status, 400);

        const invalidBody = await postBooking(
            { userId: "user-3", eventId: `${eventPrefix}-invalid`, seatNos: ["A1", "A1"] },
            "key-invalid",
        );
        assert.equal(invalidBody.response.status, 400);

        // Concurrent duplicate requests
        const concurrentResults = await Promise.all(
            Array.from({ length: 8 }, () =>
                postBooking(
                    { userId: "user-concurrent", eventId: `${eventPrefix}-concurrent`, seatNos: ["A1"] },
                    "key-concurrent",
                ),
            ),
        );
        const concurrentBookingIds = new Set(
            concurrentResults.map((result) => result.body.bookingId),
        );
        assert.equal(concurrentBookingIds.size, 1);
        assert.equal(concurrentResults.filter((result) => result.response.status === 202).length, 1);
        assert.equal(concurrentResults.filter((result) => result.response.status === 200).length, 7);

        const concurrentBookingId = [...concurrentBookingIds][0];
        const concurrentOutbox = await pool.query(
            `SELECT id FROM outbox WHERE booking_id = $1`,
            [concurrentBookingId],
        );
        assert.equal(concurrentOutbox.rows.length, 1);

        await pool.query("DELETE FROM bookings WHERE event_id LIKE $1", [`${eventPrefix}%`]);
        await pool.end();
        poolClosed = true;

        const databaseFailure = await postBooking(
            { userId: "user-db-failure", eventId: `${eventPrefix}-db-failure`, seatNos: ["A1"] },
            "key-db-failure",
        );
        assert.equal(databaseFailure.response.status, 500);

        console.log("Booking persistence and idempotency verification passed.");
    } finally {
        await new Promise<void>((resolve) => server.close(() => resolve()));
        if (!poolClosed) {
            await pool.query("DELETE FROM bookings WHERE event_id LIKE $1", [`${eventPrefix}%`]).catch(() => undefined);
            await pool.end().catch(() => undefined);
        }
    }
}

main().catch((error) => {
    console.error("Booking persistence verification failed:", error);
    process.exitCode = 1;
});
