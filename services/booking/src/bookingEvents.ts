import type {
    SeatsReservationFailedEvent,
    SeatsReservedEvent,
} from "../../../packages/common/src/messages.js";
import { pool } from "./db.js";

export type InventoryOutcomeEvent = SeatsReservedEvent | SeatsReservationFailedEvent;

export type InventoryOutcomeResult =
    | { kind: "updated"; bookingId: string; status: "SEATS_HELD" | "CANCELLED" }
    | { kind: "unknown"; bookingId: string }
    | { kind: "stale"; bookingId: string; currentStatus: string };

export class InvalidInventoryEventError extends Error {
    constructor() {
        super("Invalid Inventory outcome event");
        this.name = "InvalidInventoryEventError";
    }
}

function isRecord(value: unknown): value is Record<string, unknown> {
    return typeof value === "object" && value !== null;
}

function parseInventoryOutcomeEvent(value: unknown): InventoryOutcomeEvent {
    if (!isRecord(value)) {
        throw new InvalidInventoryEventError();
    }

    const type = value.type;
    const bookingId = value.bookingId;
    const eventId = value.eventId;
    const seatNos = value.seatNos;

    if (
        (type !== "SeatsReserved" && type !== "SeatsReservationFailed") ||
        typeof bookingId !== "string" ||
        bookingId.trim().length === 0 ||
        typeof eventId !== "string" ||
        eventId.trim().length === 0 ||
        !Array.isArray(seatNos) ||
        seatNos.length === 0 ||
        seatNos.some((seatNo) => typeof seatNo !== "string" || seatNo.trim().length === 0)
    ) {
        throw new InvalidInventoryEventError();
    }

    const normalizedSeatNos = seatNos.map((seatNo) => seatNo.trim()).sort();
    if (new Set(normalizedSeatNos).size !== normalizedSeatNos.length) {
        throw new InvalidInventoryEventError();
    }

    if (type === "SeatsReservationFailed" && value.reason !== "SEAT_UNAVAILABLE") {
        throw new InvalidInventoryEventError();
    }

    return type === "SeatsReserved"
        ? { type, bookingId, eventId, seatNos: normalizedSeatNos }
        : { type, bookingId, eventId, seatNos: normalizedSeatNos, reason: "SEAT_UNAVAILABLE" };
}

export async function applyInventoryOutcome(event: unknown): Promise<InventoryOutcomeResult> {
    const parsedEvent = parseInventoryOutcomeEvent(event);
    const nextStatus = parsedEvent.type === "SeatsReserved" ? "SEATS_HELD" : "CANCELLED";

    const updateResult = await pool.query(
        `UPDATE bookings
         SET status = $1, updated_at = NOW()
         WHERE id = $2
           AND status = 'PENDING'
           AND event_id = $3
           AND seat_nos = $4::jsonb`,
        [nextStatus, parsedEvent.bookingId, parsedEvent.eventId, JSON.stringify(parsedEvent.seatNos)],
    );

    if (updateResult.rowCount === 1) {
        return { kind: "updated", bookingId: parsedEvent.bookingId, status: nextStatus };
    }

    const existingResult = await pool.query<{ status: string }>(
        "SELECT status FROM bookings WHERE id = $1",
        [parsedEvent.bookingId],
    );

    if (existingResult.rows.length === 0) {
        return { kind: "unknown", bookingId: parsedEvent.bookingId };
    }

    return {
        kind: "stale",
        bookingId: parsedEvent.bookingId,
        currentStatus: existingResult.rows[0].status,
    };
}
