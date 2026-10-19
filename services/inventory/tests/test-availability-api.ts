import assert from "node:assert/strict";
import type { AddressInfo } from "node:net";
import { availabilityApp } from "./availabilityApi.js";
import { pool } from "./db.js";
import { confirmSeats } from "./confirmSeats.js";
import { reserveSeats } from "./reserveSeats.js";

const eventId = `test-availability-${Date.now()}`;

type AvailabilityResponse = {
    eventId: string;
    seats: Array<{ seatNo: string; status: "AVAILABLE" | "HELD" | "SOLD" }>;
};

async function main() {
    await pool.query(
        "INSERT INTO events (id, name) VALUES ($1, $1)",
        [eventId],
    );
    await pool.query(
        `INSERT INTO seats (event_id, seat_no)
         VALUES ($1, 'AVAILABLE'), ($1, 'HELD'), ($1, 'SOLD')`,
        [eventId],
    );
    assert.deepEqual(await reserveSeats("booking-held", eventId, ["HELD"]), { success: true });
    assert.deepEqual(await reserveSeats("booking-sold", eventId, ["SOLD"]), { success: true });
    assert.deepEqual(await confirmSeats("booking-sold", eventId, ["SOLD"]), { success: true });

    const server = await new Promise<ReturnType<typeof availabilityApp.listen>>((resolve) => {
        const listeningServer = availabilityApp.listen(0, () => resolve(listeningServer));
    });
    const port = (server.address() as AddressInfo).port;
    const baseUrl = `http://127.0.0.1:${port}`;
    let poolClosed = false;

    try {
        const firstResponse = await fetch(`${baseUrl}/events/${eventId}/seats`);
        assert.equal(firstResponse.status, 200);
        const firstBody = (await firstResponse.json()) as AvailabilityResponse;
        assert.deepEqual(firstBody, {
            eventId,
            seats: [
                { seatNo: "AVAILABLE", status: "AVAILABLE" },
                { seatNo: "HELD", status: "HELD" },
                { seatNo: "SOLD", status: "SOLD" },
            ],
        });

        const stateBeforeRepeat = await pool.query(
            "SELECT seat_no, status, held_by_booking FROM seats WHERE event_id = $1 ORDER BY seat_no",
            [eventId],
        );
        const repeatedResponse = await fetch(`${baseUrl}/events/${eventId}/seats`);
        assert.equal(repeatedResponse.status, 200);
        assert.deepEqual(await repeatedResponse.json(), firstBody);
        const stateAfterRepeat = await pool.query(
            "SELECT seat_no, status, held_by_booking FROM seats WHERE event_id = $1 ORDER BY seat_no",
            [eventId],
        );
        assert.deepEqual(stateAfterRepeat.rows, stateBeforeRepeat.rows);

        const unknownResponse = await fetch(`${baseUrl}/events/does-not-exist/seats`);
        assert.equal(unknownResponse.status, 404);
        assert.deepEqual(await unknownResponse.json(), { error: "Event not found" });

        const invalidResponse = await fetch(`${baseUrl}/events/${encodeURIComponent(" ")}/seats`);
        assert.equal(invalidResponse.status, 400);
        assert.deepEqual(await invalidResponse.json(), {
            error: "Event ID must be a non-empty string",
        });

        await pool.query("DELETE FROM seats WHERE event_id = $1", [eventId]);
        await pool.query("DELETE FROM events WHERE id = $1", [eventId]);
        await pool.end();
        poolClosed = true;

        const failureResponse = await fetch(`${baseUrl}/events/${eventId}/seats`);
        assert.equal(failureResponse.status, 500);
        assert.deepEqual(await failureResponse.json(), {
            error: "Failed to retrieve seat availability",
        });

        console.log("Seat availability API verification passed.");
    } finally {
        await new Promise<void>((resolve) => server.close(() => resolve()));
        if (!poolClosed) {
            await pool.query("DELETE FROM seats WHERE event_id = $1", [eventId]).catch(() => undefined);
            await pool.query("DELETE FROM events WHERE id = $1", [eventId]).catch(() => undefined);
            await pool.end().catch(() => undefined);
        }
    }
}

main().catch((error) => {
    console.error("Seat availability API verification failed:", error);
    process.exitCode = 1;
});
