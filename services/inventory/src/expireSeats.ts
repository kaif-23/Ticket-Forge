import { pool } from "./db.js";

export interface ExpiredSeat {
    eventId: string;
    seatNo: string;
}

export interface ExpiredBooking {
    bookingId: string;
    eventId: string;
    seatNos: string[];
}

export interface ExpireSeatsResult {
    expiredSeats: ExpiredSeat[];
    expiredBookings: ExpiredBooking[];
}

export async function expireHeldSeats(): Promise<ExpireSeatsResult> {
    const client = await pool.connect();
    let transactionActive = false;

    try {
        await client.query("BEGIN");
        transactionActive = true;

        const result = await client.query<{
            event_id: string;
            seat_no: string;
            held_by_booking: string | null;
        }>(
            `WITH expired AS (
                SELECT id, event_id, seat_no, held_by_booking
                FROM seats
                WHERE status = 'HELD'
                  AND hold_expires_at <= NOW()
                FOR UPDATE SKIP LOCKED
             )
             UPDATE seats s
             SET status = 'AVAILABLE',
                 held_by_booking = NULL,
                 hold_expires_at = NULL,
                 version = s.version + 1
             FROM expired e
             WHERE s.id = e.id
             RETURNING s.event_id, s.seat_no, e.held_by_booking`,
        );

        await client.query("COMMIT");
        transactionActive = false;

        const expiredSeats: ExpiredSeat[] = [];
        const bookingMap = new Map<string, ExpiredBooking>();

        for (const row of result.rows) {
            expiredSeats.push({
                eventId: row.event_id,
                seatNo: row.seat_no,
            });

            if (row.held_by_booking) {
                const key = `${row.held_by_booking}:${row.event_id}`;
                let entry = bookingMap.get(key);
                if (!entry) {
                    entry = {
                        bookingId: row.held_by_booking,
                        eventId: row.event_id,
                        seatNos: [],
                    };
                    bookingMap.set(key, entry);
                }
                entry.seatNos.push(row.seat_no);
            }
        }

        return {
            expiredSeats,
            expiredBookings: Array.from(bookingMap.values()),
        };
    } catch (error) {
        if (transactionActive) {
            try {
                await client.query("ROLLBACK");
            } catch {
                // Preserve the original database error.
            }
        }

        throw error;
    } finally {
        client.release();
    }
}
