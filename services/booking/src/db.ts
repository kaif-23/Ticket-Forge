import { Pool } from "pg";

export const pool = new Pool({
    host: process.env.BOOKING_DB_HOST ?? "localhost",
    port: Number(process.env.BOOKING_DB_PORT ?? 5433),
    database: process.env.BOOKING_DB_NAME ?? "booking_db",
    user: process.env.BOOKING_DB_USER ?? "ticketforge",
    password: process.env.BOOKING_DB_PASSWORD ?? "ticketforge",
    max: 10,
});
