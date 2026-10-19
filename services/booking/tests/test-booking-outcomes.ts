import assert from "node:assert/strict";
import { applyInventoryOutcome, InvalidInventoryEventError } from "./bookingEvents.js";
import { pool } from "./db.js";

const eventPrefix = `test-outcomes-${Date.now()}`;
const bookingIds = {
    success: `${eventPrefix}-success`,
    failure: `${eventPrefix}-failure`,
    cancelled: `${eventPrefix}-cancelled`,
    held: `${eventPrefix}-held`,
};

async function getStatus(bookingId: string) {
    const result = await pool.query(
        "SELECT status, updated_at FROM bookings WHERE id = $1",
        [bookingId],
    );
    return result.rows[0];
}

async function insertBooking(
    bookingId: string,
    status: "PENDING" | "SEATS_HELD" | "CANCELLED",
    seatNos: string[],
) {
    await pool.query(
        `INSERT INTO bookings (
            id, user_id, event_id, seat_nos, status, idempotency_key, request_hash,
            updated_at
         )
         VALUES ($1, $2, $3, $4::jsonb, $5, $6, $7, '2000-01-01T00:00:00Z')`,
        [
            bookingId,
            `${eventPrefix}-user-${bookingId}`,
            `${eventPrefix}-event`,
            JSON.stringify(seatNos),
            status,
            `${eventPrefix}-key-${bookingId}`,
            `${eventPrefix}-hash-${bookingId}`,
        ],
    );
}

async function main() {
    await insertBooking(bookingIds.success, "PENDING", ["A1", "A2"]);
    await insertBooking(bookingIds.failure, "PENDING", ["B1"]);
    await insertBooking(bookingIds.cancelled, "CANCELLED", ["C1"]);
    await insertBooking(bookingIds.held, "SEATS_HELD", ["D1"]);

    try {
        const success = await applyInventoryOutcome({
            type: "SeatsReserved",
            bookingId: bookingIds.success,
            eventId: `${eventPrefix}-event`,
            seatNos: ["A2", "A1"],
        });
        assert.deepEqual(success, {
            kind: "updated",
            bookingId: bookingIds.success,
            status: "SEATS_HELD",
        });
        const successfulBooking = await getStatus(bookingIds.success);
        assert.equal(successfulBooking.status, "SEATS_HELD");
        assert.ok(successfulBooking.updated_at > new Date("2000-01-01T00:00:00Z"));

        const failure = await applyInventoryOutcome({
            type: "SeatsReservationFailed",
            bookingId: bookingIds.failure,
            eventId: `${eventPrefix}-event`,
            seatNos: ["B1"],
            reason: "SEAT_UNAVAILABLE",
        });
        assert.deepEqual(failure, {
            kind: "updated",
            bookingId: bookingIds.failure,
            status: "CANCELLED",
        });

        const unknown = await applyInventoryOutcome({
            type: "SeatsReserved",
            bookingId: `${eventPrefix}-unknown`,
            eventId: `${eventPrefix}-event`,
            seatNos: ["UNKNOWN"],
        });
        assert.deepEqual(unknown, {
            kind: "unknown",
            bookingId: `${eventPrefix}-unknown`,
        });
        const unknownCount = await pool.query(
            "SELECT COUNT(*)::int AS count FROM bookings WHERE id = $1",
            [`${eventPrefix}-unknown`],
        );
        assert.equal(unknownCount.rows[0].count, 0);

        const repeatedSuccess = await applyInventoryOutcome({
            type: "SeatsReserved",
            bookingId: bookingIds.success,
            eventId: `${eventPrefix}-event`,
            seatNos: ["A1", "A2"],
        });
        assert.deepEqual(repeatedSuccess, {
            kind: "stale",
            bookingId: bookingIds.success,
            currentStatus: "SEATS_HELD",
        });

        const repeatedFailure = await applyInventoryOutcome({
            type: "SeatsReservationFailed",
            bookingId: bookingIds.failure,
            eventId: `${eventPrefix}-event`,
            seatNos: ["B1"],
            reason: "SEAT_UNAVAILABLE",
        });
        assert.deepEqual(repeatedFailure, {
            kind: "stale",
            bookingId: bookingIds.failure,
            currentStatus: "CANCELLED",
        });

        const staleSuccess = await applyInventoryOutcome({
            type: "SeatsReserved",
            bookingId: bookingIds.cancelled,
            eventId: `${eventPrefix}-event`,
            seatNos: ["C1"],
        });
        assert.deepEqual(staleSuccess, {
            kind: "stale",
            bookingId: bookingIds.cancelled,
            currentStatus: "CANCELLED",
        });

        const staleFailure = await applyInventoryOutcome({
            type: "SeatsReservationFailed",
            bookingId: bookingIds.held,
            eventId: `${eventPrefix}-event`,
            seatNos: ["D1"],
            reason: "SEAT_UNAVAILABLE",
        });
        assert.deepEqual(staleFailure, {
            kind: "stale",
            bookingId: bookingIds.held,
            currentStatus: "SEATS_HELD",
        });

        const mismatchedEvent = await applyInventoryOutcome({
            type: "SeatsReserved",
            bookingId: bookingIds.success,
            eventId: `${eventPrefix}-wrong-event`,
            seatNos: ["A1", "A2"],
        });
        assert.deepEqual(mismatchedEvent, {
            kind: "stale",
            bookingId: bookingIds.success,
            currentStatus: "SEATS_HELD",
        });

        for (const invalidEvent of [
            null,
            {},
            { type: "SeatsReserved", bookingId: bookingIds.success },
            {
                type: "SeatsReservationFailed",
                bookingId: bookingIds.success,
                eventId: `${eventPrefix}-event`,
                seatNos: ["A1"],
                reason: "OTHER",
            },
            {
                type: "SeatsReserved",
                bookingId: bookingIds.success,
                eventId: `${eventPrefix}-event`,
                seatNos: ["A1", "A1"],
            },
        ]) {
            await assert.rejects(
                () => applyInventoryOutcome(invalidEvent),
                InvalidInventoryEventError,
            );
        }

        await pool.query("DELETE FROM bookings WHERE id LIKE $1", [`${eventPrefix}%`]);
        await pool.end();

        await assert.rejects(
            () =>
                applyInventoryOutcome({
                    type: "SeatsReserved",
                    bookingId: bookingIds.success,
                    eventId: `${eventPrefix}-event`,
                    seatNos: ["A1", "A2"],
                }),
            /pool after calling end/,
        );

        console.log("Booking Inventory outcome verification passed.");
    } finally {
        await pool.end().catch(() => undefined);
    }
}

main().catch((error) => {
    console.error("Booking Inventory outcome verification failed:", error);
    process.exitCode = 1;
});
