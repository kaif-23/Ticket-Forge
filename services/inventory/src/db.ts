import { Pool } from "pg";

export const pool = new Pool({
  host: "localhost",
  port: 5434,
  database: "inventory_db",
  user: "ticketforge",
  password: "ticketforge",
  max: 10,
});