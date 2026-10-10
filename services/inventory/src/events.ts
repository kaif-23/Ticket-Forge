import { pool } from "./db.js";

export interface EventRecord {
    id: string;
    name: string;
    startsAt: Date | null;
    createdAt: Date;
}

export class InvalidEventError extends Error {
    constructor(message: string) {
        super(message);
        this.name = "InvalidEventError";
    }
}

export class EventNotFoundError extends Error {
    constructor() {
        super("Event not found");
        this.name = "EventNotFoundError";
    }
}

export class EventAlreadyExistsError extends Error {
    constructor() {
        super("Event already exists");
        this.name = "EventAlreadyExistsError";
    }
}

function rowToEvent(row: Record<string, unknown>): EventRecord {
    return {
        id: row.id as string,
        name: row.name as string,
        startsAt: row.starts_at as Date | null,
        createdAt: row.created_at as Date,
    };
}

export function validateEventId(eventId: string): string {
    const normalizedEventId = eventId.trim();
    if (!normalizedEventId) {
        throw new InvalidEventError("Event ID must be a non-empty string");
    }
    return normalizedEventId;
}

export function normalizeEventInput(input: unknown): {
    id: string;
    name: string;
    startsAt: Date | null;
} {
    if (!input || typeof input !== "object") {
        throw new InvalidEventError("Request body must be an object");
    }

    const body = input as Record<string, unknown>;
    const id = typeof body.id === "string" ? validateEventId(body.id) : "";
    const name = typeof body.name === "string" ? body.name.trim() : "";

    if (!id) {
        throw new InvalidEventError("id must be a non-empty string");
    }
    if (!name) {
        throw new InvalidEventError("name must be a non-empty string");
    }

    if (body.startsAt === undefined || body.startsAt === null || body.startsAt === "") {
        return { id, name, startsAt: null };
    }

    if (typeof body.startsAt !== "string" || Number.isNaN(Date.parse(body.startsAt))) {
        throw new InvalidEventError("startsAt must be a valid date string");
    }

    return { id, name, startsAt: new Date(body.startsAt) };
}

export async function listEvents(): Promise<EventRecord[]> {
    const result = await pool.query(
        `SELECT id, name, starts_at, created_at
         FROM events
         ORDER BY created_at, id`,
    );
    return result.rows.map(rowToEvent);
}

export async function getEventById(eventId: string): Promise<EventRecord | null> {
    const result = await pool.query(
        `SELECT id, name, starts_at, created_at
         FROM events
         WHERE id = $1`,
        [validateEventId(eventId)],
    );
    return result.rows.length === 0 ? null : rowToEvent(result.rows[0]);
}

export async function createEvent(input: unknown): Promise<EventRecord> {
    const event = normalizeEventInput(input);

    try {
        const result = await pool.query(
            `INSERT INTO events (id, name, starts_at)
             VALUES ($1, $2, $3)
             RETURNING id, name, starts_at, created_at`,
            [event.id, event.name, event.startsAt],
        );
        return rowToEvent(result.rows[0]);
    } catch (error) {
        if ((error as { code?: string }).code === "23505") {
            throw new EventAlreadyExistsError();
        }
        throw error;
    }
}
