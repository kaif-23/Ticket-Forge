import { Pool } from "pg";

export const pool = new Pool({
    host: process.env.PAYMENT_DB_HOST ?? "localhost",
    port: Number(process.env.PAYMENT_DB_PORT ?? 5435),
    database: process.env.PAYMENT_DB_NAME ?? "payment_db",
    user: process.env.PAYMENT_DB_USER ?? "ticketforge",
    password: process.env.PAYMENT_DB_PASSWORD ?? "ticketforge",
    max: 10,
});
