import type { PoolClient } from "pg";
import { pool } from "./db.js";

export type ConfirmSeatsResult =
    | { success: true }
    | { success: false; reason: "SEAT_UNAVAILABLE" };

export async function confirmSeats(
    bookingId: string,
    eventId: string,
    seatNos: string[],
    client?: PoolClient,
): Promise<ConfirmSeatsResult> {
    const uniqueSeatNos = [...new Set(seatNos)];

    if (uniqueSeatNos.length === 0 || uniqueSeatNos.length !== seatNos.length) {
        return { success: false, reason: "SEAT_UNAVAILABLE" };
    }

    const localClient = client ?? (await pool.connect());
    let transactionActive = false;

    try {
        if (!client) {
            await localClient.query("BEGIN");
            transactionActive = true;
        }

        const result = await localClient.query<{ seat_no: string }>(
            `UPDATE seats
       SET status = 'SOLD',
           held_by_booking = NULL,
           hold_expires_at = NULL,
           version = version + 1
       WHERE event_id = $2
         AND seat_no = ANY($3::text[])
         AND status = 'HELD'
         AND held_by_booking = $1
         AND hold_expires_at > NOW()
       RETURNING seat_no`,
            [bookingId, eventId, uniqueSeatNos],
        );

        if (result.rowCount !== uniqueSeatNos.length) {
            if (!client && transactionActive) {
                await localClient.query("ROLLBACK");
                transactionActive = false;
            }
            return { success: false, reason: "SEAT_UNAVAILABLE" };
        }

        if (!client) {
            await localClient.query("COMMIT");
            transactionActive = false;
        }
        return { success: true };
    } catch (error) {
        if (!client && transactionActive) {
            try {
                await localClient.query("ROLLBACK");
            } catch {
                // Preserve the original database error.
            }
        }

        throw error;
    } finally {
        if (!client) {
            localClient.release();
        }
    }
}
