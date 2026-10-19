import assert from "node:assert/strict";
import type { AddressInfo } from "node:net";
import { availabilityApp } from "./availabilityApi.js";
import { pool } from "./db.js";

const eventId = `test-event-api-${Date.now()}`;

async function main() {
    const server = await new Promise<ReturnType<typeof availabilityApp.listen>>((resolve) => {
        const listeningServer = availabilityApp.listen(0, () => resolve(listeningServer));
    });
    const port = (server.address() as AddressInfo).port;
    const baseUrl = `http://127.0.0.1:${port}`;
    let poolClosed = false;

    async function postEvent(body: unknown) {
        const response = await fetch(`${baseUrl}/admin/events`, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(body),
        });
        return { response, body: await response.json() };
    }

    try {
        const created = await postEvent({
            id: eventId,
            name: "Test Event",
            startsAt: "2027-01-01T12:00:00Z",
        });
        assert.equal(created.response.status, 201);
        assert.equal(created.body.id, eventId);
        assert.equal(created.body.name, "Test Event");
        assert.equal(created.body.startsAt, "2027-01-01T12:00:00.000Z");

        const duplicate = await postEvent({ id: eventId, name: "Duplicate" });
        assert.equal(duplicate.response.status, 409);
        assert.deepEqual(duplicate.body, { error: "Event already exists" });

        const invalid = await postEvent({ id: " ", name: "Invalid" });
        assert.equal(invalid.response.status, 400);

        const listResponse = await fetch(`${baseUrl}/events`);
        assert.equal(listResponse.status, 200);
        const listedEvents = (await listResponse.json()) as Array<{ id: string }>;
        assert.ok(listedEvents.some((event) => event.id === eventId));

        const detailResponse = await fetch(`${baseUrl}/events/${eventId}`);
        assert.equal(detailResponse.status, 200);
        assert.equal((await detailResponse.json()).id, eventId);

        const missingResponse = await fetch(`${baseUrl}/events/does-not-exist`);
        assert.equal(missingResponse.status, 404);
        assert.deepEqual(await missingResponse.json(), { error: "Event not found" });

        const invalidIdResponse = await fetch(`${baseUrl}/events/%20`);
        assert.equal(invalidIdResponse.status, 400);
        assert.deepEqual(await invalidIdResponse.json(), {
            error: "Event ID must be a non-empty string",
        });

        const zeroSeatResponse = await fetch(`${baseUrl}/events/${eventId}/seats`);
        assert.equal(zeroSeatResponse.status, 200);
        assert.deepEqual(await zeroSeatResponse.json(), { eventId, seats: [] });

        await pool.query(
            "INSERT INTO seats (event_id, seat_no) VALUES ($1, 'A1'), ($1, 'A2')",
            [eventId],
        );
        const seatsResponse = await fetch(`${baseUrl}/events/${eventId}/seats`);
        assert.equal(seatsResponse.status, 200);
        assert.deepEqual(await seatsResponse.json(), {
            eventId,
            seats: [
                { seatNo: "A1", status: "AVAILABLE" },
                { seatNo: "A2", status: "AVAILABLE" },
            ],
        });

        await assert.rejects(
            pool.query(
                "INSERT INTO seats (event_id, seat_no) VALUES ('missing-event', 'ORPHAN')",
            ),
            (error: unknown) =>
                typeof error === "object" &&
                error !== null &&
                "code" in error &&
                error.code === "23503",
        );

        await pool.query("DELETE FROM seats WHERE event_id = $1", [eventId]);
        await pool.query("DELETE FROM events WHERE id = $1", [eventId]);
        await pool.end();
        poolClosed = true;

        const databaseFailureResponse = await fetch(`${baseUrl}/events`);
        assert.equal(databaseFailureResponse.status, 500);
        assert.deepEqual(await databaseFailureResponse.json(), {
            error: "Failed to list events",
        });

        console.log("Inventory event API verification passed.");
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
    console.error("Inventory event API verification failed:", error);
    process.exitCode = 1;
});
