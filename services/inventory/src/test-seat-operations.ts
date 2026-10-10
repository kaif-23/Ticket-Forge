import assert from "node:assert/strict";
import { confirmSeats } from "./confirmSeats.js";
import { pool } from "./db.js";
import { releaseSeats } from "./releaseSeats.js";
import { reserveSeats } from "./reserveSeats.js";

const eventId = `test-operations-${Date.now()}`;

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
         VALUES ($1, 'CONFIRM-1'), ($1, 'CONFIRM-2'), ($1, 'OTHER'),
                ($1, 'ATOMIC-CONFIRM'), ($1, 'EXPIRED'), ($1, 'RELEASE'),
                ($1, 'ATOMIC-RELEASE'), ($1, 'CONCURRENT')`,
        [eventId],
    );

    try {
        assert.deepEqual(
            await reserveSeats("booking-confirm", eventId, ["CONFIRM-1", "CONFIRM-2"]),
            { success: true },
        );
        assert.deepEqual(
            await confirmSeats("booking-confirm", eventId, ["CONFIRM-1", "CONFIRM-2"]),
            { success: true },
        );
        for (const seatNo of ["CONFIRM-1", "CONFIRM-2"]) {
            const seat = await getSeat(seatNo);
            assert.equal(seat.status, "SOLD");
            assert.equal(seat.held_by_booking, null);
            assert.equal(seat.hold_expires_at, null);
        }

        assert.deepEqual(await confirmSeats("booking-confirm", eventId, ["CONFIRM-1"]), {
            success: false,
            reason: "SEAT_UNAVAILABLE",
        });

        assert.deepEqual(await reserveSeats("booking-owner", eventId, ["OTHER"]), { success: true });
        assert.deepEqual(await confirmSeats("booking-other", eventId, ["OTHER"]), {
            success: false,
            reason: "SEAT_UNAVAILABLE",
        });
        assert.equal((await getSeat("OTHER")).status, "HELD");

        assert.deepEqual(await reserveSeats("booking-atomic-confirm", eventId, ["ATOMIC-CONFIRM"]), {
            success: true,
        });
        assert.deepEqual(
            await confirmSeats("booking-atomic-confirm", eventId, ["ATOMIC-CONFIRM", "MISSING"]),
            { success: false, reason: "SEAT_UNAVAILABLE" },
        );
        assert.equal((await getSeat("ATOMIC-CONFIRM")).status, "HELD");

        assert.deepEqual(await reserveSeats("booking-expired", eventId, ["EXPIRED"]), { success: true });
        await pool.query(
            "UPDATE seats SET hold_expires_at = NOW() - INTERVAL '1 minute' WHERE event_id = $1 AND seat_no = 'EXPIRED'",
            [eventId],
        );
        assert.deepEqual(await confirmSeats("booking-expired", eventId, ["EXPIRED"]), {
            success: false,
            reason: "SEAT_UNAVAILABLE",
        });
        assert.equal((await getSeat("EXPIRED")).status, "HELD");

        assert.deepEqual(await reserveSeats("booking-release", eventId, ["RELEASE"]), { success: true });
        assert.deepEqual(await releaseSeats("booking-release", eventId, ["RELEASE"]), { success: true });
        const releasedSeat = await getSeat("RELEASE");
        assert.equal(releasedSeat.status, "AVAILABLE");
        assert.equal(releasedSeat.held_by_booking, null);
        assert.equal(releasedSeat.hold_expires_at, null);

        assert.deepEqual(await reserveSeats("booking-release-owner", eventId, ["ATOMIC-RELEASE"]), {
            success: true,
        });
        assert.deepEqual(await releaseSeats("booking-release-other", eventId, ["ATOMIC-RELEASE"]), {
            success: false,
            reason: "SEAT_UNAVAILABLE",
        });
        assert.deepEqual(
            await releaseSeats("booking-release-owner", eventId, ["ATOMIC-RELEASE", "MISSING"]),
            { success: false, reason: "SEAT_UNAVAILABLE" },
        );
        assert.equal((await getSeat("ATOMIC-RELEASE")).status, "HELD");

        assert.deepEqual(await releaseSeats("booking-confirm", eventId, ["CONFIRM-1"]), {
            success: false,
            reason: "SEAT_UNAVAILABLE",
        });

        assert.deepEqual(await reserveSeats("booking-concurrent", eventId, ["CONCURRENT"]), {
            success: true,
        });
        const concurrentResults = await Promise.all([
            confirmSeats("booking-concurrent", eventId, ["CONCURRENT"]),
            releaseSeats("booking-concurrent", eventId, ["CONCURRENT"]),
        ]);
        assert.equal(concurrentResults.filter((result) => result.success).length, 1);
        assert.equal(
            concurrentResults.filter(
                (result) => !result.success && result.reason === "SEAT_UNAVAILABLE",
            ).length,
            1,
        );
        const concurrentSeat = await getSeat("CONCURRENT");
        assert.ok(["SOLD", "AVAILABLE"].includes(concurrentSeat.status));
        assert.equal(concurrentSeat.held_by_booking, null);
        assert.equal(concurrentSeat.hold_expires_at, null);

        console.log("Seat confirmation and release verification passed.");
    } finally {
        await pool.query("DELETE FROM seats WHERE event_id = $1", [eventId]);
        await pool.query("DELETE FROM events WHERE id = $1", [eventId]);
        await pool.end();
    }
}

main().catch((error) => {
    console.error("Seat confirmation and release verification failed:", error);
    process.exitCode = 1;
});
