import assert from "node:assert/strict";
import { confirmSeats } from "../src/confirmSeats.js";
import { expireHeldSeats } from "../src/expireSeats.js";
import { pool } from "../src/db.js";
import { reserveSeats } from "../src/reserveSeats.js";

const eventId = `test-expiry-${Date.now()}`;

async function getSeat(seatNo: string) {
    const result = await pool.query(
        `SELECT status, held_by_booking, hold_expires_at, version
         FROM seats
         WHERE event_id = $1 AND seat_no = $2`,
        [eventId, seatNo],
    );
    return result.rows[0];
}

async function main() {
    await pool.query(
        "INSERT INTO events (id, name) VALUES ($1, $1)",
        [eventId],
    );
    await pool.query(
        `INSERT INTO seats (event_id, seat_no)
         VALUES ($1, 'EXPIRED-1'), ($1, 'EXPIRED-2'), ($1, 'FUTURE'),
                ($1, 'AVAILABLE-OLD'), ($1, 'SOLD-OLD'), ($1, 'RACE')`,
        [eventId],
    );

    try {
        assert.deepEqual(await reserveSeats("booking-expired", eventId, ["EXPIRED-1", "EXPIRED-2"]), {
            success: true,
        });
        assert.deepEqual(await reserveSeats("booking-future", eventId, ["FUTURE"]), { success: true });
        assert.deepEqual(await reserveSeats("booking-race", eventId, ["RACE"]), { success: true });

        await pool.query(
            `UPDATE seats
             SET hold_expires_at = CASE seat_no
                 WHEN 'EXPIRED-1' THEN NOW() - INTERVAL '2 minutes'
                 WHEN 'EXPIRED-2' THEN NOW()
                 WHEN 'RACE' THEN NOW() - INTERVAL '2 minutes'
                 ELSE hold_expires_at
             END
             WHERE event_id = $1`,
            [eventId],
        );
        await pool.query(
            `UPDATE seats
             SET status = 'AVAILABLE', held_by_booking = NULL, hold_expires_at = NOW() - INTERVAL '2 minutes'
             WHERE event_id = $1 AND seat_no = 'AVAILABLE-OLD'`,
            [eventId],
        );
        await pool.query(
            `UPDATE seats
             SET status = 'SOLD', held_by_booking = 'booking-sold', hold_expires_at = NOW() - INTERVAL '2 minutes'
             WHERE event_id = $1 AND seat_no = 'SOLD-OLD'`,
            [eventId],
        );

        const expiryResult = await expireHeldSeats();
        const testEventExpiredSeats = expiryResult.expiredSeats
            .filter((seat) => seat.eventId === eventId)
            .sort((left, right) => left.seatNo.localeCompare(right.seatNo));
        assert.deepEqual(
            testEventExpiredSeats,
            [
                { eventId, seatNo: "EXPIRED-1" },
                { eventId, seatNo: "EXPIRED-2" },
                { eventId, seatNo: "RACE" },
            ],
        );

        for (const seatNo of ["EXPIRED-1", "EXPIRED-2", "RACE"]) {
            const seat = await getSeat(seatNo);
            assert.equal(seat.status, "AVAILABLE");
            assert.equal(seat.held_by_booking, null);
            assert.equal(seat.hold_expires_at, null);
        }
        assert.equal((await getSeat("FUTURE")).status, "HELD");
        assert.equal((await getSeat("AVAILABLE-OLD")).status, "AVAILABLE");
        assert.equal((await getSeat("SOLD-OLD")).status, "SOLD");

        const repeatedResult = await expireHeldSeats();
        assert.deepEqual(
            repeatedResult.expiredSeats.filter((seat) => seat.eventId === eventId),
            [],
        );

        assert.deepEqual(await reserveSeats("booking-race-2", eventId, ["RACE"]), { success: true });
        await pool.query(
            "UPDATE seats SET hold_expires_at = NOW() - INTERVAL '1 minute' WHERE event_id = $1 AND seat_no = 'RACE'",
            [eventId],
        );
        const concurrentResults = await Promise.all([
            expireHeldSeats(),
            confirmSeats("booking-race-2", eventId, ["RACE"]),
        ]);
        assert.equal(
            concurrentResults[0].expiredSeats.filter((seat) => seat.eventId === eventId).length,
            1,
        );
        assert.deepEqual(concurrentResults[1], {
            success: false,
            reason: "SEAT_UNAVAILABLE",
        });
        assert.equal((await getSeat("RACE")).status, "AVAILABLE");

        await pool.query("SELECT 1");
        console.log("Seat expiry verification passed.");
    } finally {
        await pool.query("DELETE FROM seats WHERE event_id = $1", [eventId]);
        await pool.query("DELETE FROM events WHERE id = $1", [eventId]);
        await pool.end();
    }
}

main().catch((error) => {
    console.error("Seat expiry verification failed:", error);
    process.exitCode = 1;
});
