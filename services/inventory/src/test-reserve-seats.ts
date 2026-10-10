import assert from "node:assert/strict";
import { pool } from "./db.js";
import { reserveSeats } from "./reserveSeats.js";

const eventId = `test-event-${Date.now()}`;

async function main() {
    await pool.query(
        `INSERT INTO seats (event_id, seat_no)
         VALUES ($1, 'A1'), ($1, 'A2'), ($1, 'A3'), ($1, 'A4'), ($1, 'A5'), ($1, 'CONCURRENT')`,
        [eventId],
    );

    try {
        const successfulReservation = await reserveSeats("booking-success", eventId, ["A1", "A2"]);
        assert.deepEqual(successfulReservation, { success: true });

        const heldSeats = await pool.query(
            `SELECT seat_no, status, held_by_booking, hold_expires_at, version
             FROM seats
             WHERE event_id = $1
             ORDER BY seat_no`,
            [eventId],
        );
        const successfulRows = heldSeats.rows.filter((row) => ["A1", "A2"].includes(row.seat_no));
        assert.equal(successfulRows.length, 2);
        assert.ok(successfulRows.every((row) => row.status === "HELD"));
        assert.ok(successfulRows.every((row) => row.held_by_booking === "booking-success"));
        assert.ok(successfulRows.every((row) => row.hold_expires_at > new Date()));
        assert.ok(successfulRows.every((row) => row.version === 1));

        const unavailableReservation = await reserveSeats("booking-unavailable", eventId, ["A1", "A3"]);
        assert.deepEqual(unavailableReservation, {
            success: false,
            reason: "SEAT_UNAVAILABLE",
        });

        const availableSeatAfterRollback = await pool.query(
            "SELECT status FROM seats WHERE event_id = $1 AND seat_no = 'A3'",
            [eventId],
        );
        assert.equal(availableSeatAfterRollback.rows[0].status, "AVAILABLE");

        const unknownSeatReservation = await reserveSeats("booking-unknown", eventId, ["A4", "MISSING"]);
        assert.deepEqual(unknownSeatReservation, {
            success: false,
            reason: "SEAT_UNAVAILABLE",
        });

        const availableSeatAfterUnknown = await pool.query(
            "SELECT status FROM seats WHERE event_id = $1 AND seat_no = 'A4'",
            [eventId],
        );
        assert.equal(availableSeatAfterUnknown.rows[0].status, "AVAILABLE");

        const duplicateReservation = await reserveSeats("booking-duplicate", eventId, ["A5", "A5"]);
        assert.deepEqual(duplicateReservation, {
            success: false,
            reason: "SEAT_UNAVAILABLE",
        });

        const concurrentResults = await Promise.all([
            reserveSeats("booking-concurrent-1", eventId, ["CONCURRENT"]),
            reserveSeats("booking-concurrent-2", eventId, ["CONCURRENT"]),
        ]);
        assert.equal(concurrentResults.filter((result) => result.success).length, 1);
        assert.equal(
            concurrentResults.filter(
                (result) => !result.success && result.reason === "SEAT_UNAVAILABLE",
            ).length,
            1,
        );

        console.log("Reservation verification passed.");
    } finally {
        await pool.query("DELETE FROM seats WHERE event_id = $1", [eventId]);
        await pool.end();
    }
}

main().catch((error) => {
    console.error("Reservation verification failed:", error);
    process.exitCode = 1;
});
