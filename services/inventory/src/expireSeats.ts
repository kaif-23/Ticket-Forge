import { pool } from "./db.js";

export interface ExpiredSeat {
    eventId: string;
    seatNo: string;
}

export interface ExpireSeatsResult {
    expiredSeats: ExpiredSeat[];
}

export async function expireHeldSeats(): Promise<ExpireSeatsResult> {
    const client = await pool.connect();
    let transactionActive = false;

    try {
        await client.query("BEGIN");
        transactionActive = true;

        const result = await client.query<{ event_id: string; seat_no: string }>(
            `UPDATE seats
       SET status = 'AVAILABLE',
           held_by_booking = NULL,
           hold_expires_at = NULL,
           version = version + 1
       WHERE status = 'HELD'
         AND hold_expires_at <= NOW()
       RETURNING event_id, seat_no`,
        );

        await client.query("COMMIT");
        transactionActive = false;

        return {
            expiredSeats: result.rows.map((row) => ({
                eventId: row.event_id,
                seatNo: row.seat_no,
            })),
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
