import { pool } from "./db.js";
import { EventNotFoundError, getEventById, validateEventId } from "./events.js";

export type SeatStatus = "AVAILABLE" | "HELD" | "SOLD";

export interface SeatAvailability {
    seatNo: string;
    status: SeatStatus;
}

export class InvalidEventIdError extends Error {
    constructor() {
        super("Event ID must be a non-empty string");
        this.name = "InvalidEventIdError";
    }
}

export async function getSeatAvailability(eventId: string): Promise<SeatAvailability[]> {
    try {
        validateEventId(eventId);
    } catch {
        throw new InvalidEventIdError();
    }

    if (!(await getEventById(eventId))) {
        throw new EventNotFoundError();
    }

    const result = await pool.query<{ seat_no: string; status: SeatStatus }>(
        `SELECT seat_no, status
         FROM seats
         WHERE event_id = $1
         ORDER BY seat_no`,
        [eventId],
    );

    return result.rows.map((row) => ({
        seatNo: row.seat_no,
        status: row.status,
    }));
}
