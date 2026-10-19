import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import type { AddressInfo } from "node:net";
import { createBookingApp } from "../src/bookingApi.js";
import { pool } from "../src/db.js";

const bookingId = randomUUID();
const eventId = `test-read-event-${Date.now()}`;

async function main() {
    await pool.query(
        `INSERT INTO bookings (
            id, user_id, event_id, seat_nos, status, idempotency_key, request_hash,
            created_at, updated_at
         )
         VALUES ($1, 'test-read-user', $2, '["A1", "A2"]'::jsonb,
                 'SEATS_HELD', 'test-read-key', 'test-read-hash',
                 '2026-01-01T00:00:00Z', '2026-01-01T00:01:00Z')`,
        [bookingId, eventId],
    );

    const app = createBookingApp(async () => undefined);
    const server = await new Promise<ReturnType<typeof app.listen>>((resolve) => {
        const listeningServer = app.listen(0, () => resolve(listeningServer));
    });
    const port = (server.address() as AddressInfo).port;
    const baseUrl = `http://127.0.0.1:${port}`;
    let poolClosed = false;

    try {
        const existingResponse = await fetch(`${baseUrl}/bookings/${bookingId}`);
        assert.equal(existingResponse.status, 200);
        assert.deepEqual(await existingResponse.json(), {
            id: bookingId,
            userId: "test-read-user",
            eventId,
            seatNos: ["A1", "A2"],
            status: "SEATS_HELD",
            createdAt: "2026-01-01T00:00:00.000Z",
            updatedAt: "2026-01-01T00:01:00.000Z",
        });

        const missingResponse = await fetch(`${baseUrl}/bookings/${randomUUID()}`);
        assert.equal(missingResponse.status, 404);
        assert.deepEqual(await missingResponse.json(), { error: "Booking not found" });

        const invalidResponse = await fetch(`${baseUrl}/bookings/not-a-booking-id`);
        assert.equal(invalidResponse.status, 400);
        assert.deepEqual(await invalidResponse.json(), {
            error: "Booking ID must be a valid UUID",
        });

        await pool.query("DELETE FROM bookings WHERE id = $1", [bookingId]);
        await pool.end();
        poolClosed = true;

        const databaseFailureResponse = await fetch(`${baseUrl}/bookings/${bookingId}`);
        assert.equal(databaseFailureResponse.status, 500);
        assert.deepEqual(await databaseFailureResponse.json(), {
            error: "Failed to retrieve booking",
        });

        console.log("Booking read endpoint verification passed.");
    } finally {
        await new Promise<void>((resolve) => server.close(() => resolve()));
        if (!poolClosed) {
            await pool.query("DELETE FROM bookings WHERE id = $1", [bookingId]).catch(() => undefined);
            await pool.end().catch(() => undefined);
        }
    }
}

main().catch((error) => {
    console.error("Booking read endpoint verification failed:", error);
    process.exitCode = 1;
});
