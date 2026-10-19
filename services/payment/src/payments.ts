import { randomUUID } from "node:crypto";
import type { Pool, PoolClient } from "pg";
import type {
    PaymentFailedEvent,
    PaymentSucceededEvent,
    ProcessPaymentCommand,
} from "../../../packages/common/src/messages.js";

import { recordProcessedMessage } from "./idempotency.js";

export type PaymentStatus = "SUCCEEDED" | "FAILED" | "REFUNDED";

export interface PaymentRecord {
    id: string;
    bookingId: string;
    status: PaymentStatus;
    amount: number;
    currency: string;
    reason: string | null;
    createdAt: Date;
    updatedAt: Date;
}

function rowToPaymentRecord(row: Record<string, unknown>): PaymentRecord {
    return {
        id: row.id as string,
        bookingId: row.booking_id as string,
        status: row.status as PaymentStatus,
        amount: Number(row.amount),
        currency: (row.currency as string) ?? "USD",
        reason: (row.reason as string | null) ?? null,
        createdAt: row.created_at as Date,
        updatedAt: row.updated_at as Date,
    };
}

export async function getPaymentByBookingId(
    clientOrPool: Pool | PoolClient,
    bookingId: string,
): Promise<PaymentRecord | null> {
    const res = await clientOrPool.query(
        `SELECT id, booking_id, status, amount, currency, reason, created_at, updated_at
         FROM payments
         WHERE booking_id = $1`,
        [bookingId],
    );

    if (res.rows.length === 0) return null;
    return rowToPaymentRecord(res.rows[0]);
}

export interface ProcessPaymentInput extends ProcessPaymentCommand {
    payload: {
        amount: number;
        currency?: string;
        simulateFailure?: boolean;
    };
}

export interface PaymentExecutionResult {
    payment: PaymentRecord;
    isDuplicate: boolean;
    event: PaymentSucceededEvent | PaymentFailedEvent;
}

export async function executePayment(
    pool: Pool,
    command: ProcessPaymentInput,
    consumerGroup = "payment-service-group",
    topic = "payment.commands",
): Promise<PaymentExecutionResult> {
    const client = await pool.connect();
    try {
        await client.query("BEGIN");

        // 1. Check message-level idempotency in processed_messages
        const { isDuplicate: isMessageDuplicate } = await recordProcessedMessage(
            client,
            command.messageId,
            consumerGroup,
            topic,
        );

        if (isMessageDuplicate) {
            await client.query("ROLLBACK");
            const existingPayment = await getPaymentByBookingId(pool, command.bookingId);
            if (existingPayment) {
                const event: PaymentSucceededEvent | PaymentFailedEvent =
                    existingPayment.status === "SUCCEEDED"
                        ? {
                              messageId: randomUUID(),
                              bookingId: existingPayment.bookingId,
                              type: "PaymentSucceeded",
                              occurredAt: new Date().toISOString(),
                              payload: {
                                  paymentId: existingPayment.id,
                                  amount: existingPayment.amount,
                              },
                          }
                        : {
                              messageId: randomUUID(),
                              bookingId: existingPayment.bookingId,
                              type: "PaymentFailed",
                              occurredAt: new Date().toISOString(),
                              payload: {
                                  reason: existingPayment.reason ?? "PAYMENT_DECLINED",
                              },
                          };
                return { payment: existingPayment, isDuplicate: true, event };
            }
        }

        // 2. Check if a payment for this booking already exists (Booking-level check)
        const existingRes = await client.query(
            `SELECT id, booking_id, status, amount, currency, reason, created_at, updated_at
             FROM payments
             WHERE booking_id = $1
             FOR UPDATE`,
            [command.bookingId],
        );

        if (existingRes.rows.length > 0) {
            const existingPayment = rowToPaymentRecord(existingRes.rows[0]);
            await client.query("COMMIT");

            // Return existing outcome without duplicating payment effect
            if (existingPayment.status === "SUCCEEDED") {
                const event: PaymentSucceededEvent = {
                    messageId: randomUUID(),
                    bookingId: existingPayment.bookingId,
                    type: "PaymentSucceeded",
                    occurredAt: new Date().toISOString(),
                    payload: {
                        paymentId: existingPayment.id,
                        amount: existingPayment.amount,
                    },
                };
                return { payment: existingPayment, isDuplicate: true, event };
            } else {
                const event: PaymentFailedEvent = {
                    messageId: randomUUID(),
                    bookingId: existingPayment.bookingId,
                    type: "PaymentFailed",
                    occurredAt: new Date().toISOString(),
                    payload: {
                        reason: existingPayment.reason ?? "PAYMENT_DECLINED",
                    },
                };
                return { payment: existingPayment, isDuplicate: true, event };
            }
        }

        // 2. New payment processing
        const amount = command.payload.amount;
        const currency = command.payload.currency ?? "USD";
        const shouldFail =
            amount <= 0 ||
            command.payload.simulateFailure === true ||
            command.bookingId.toLowerCase().includes("fail");

        const paymentId = randomUUID();
        const status: PaymentStatus = shouldFail ? "FAILED" : "SUCCEEDED";
        const reason: string | null = shouldFail
            ? amount <= 0
                ? "INVALID_AMOUNT"
                : "PAYMENT_DECLINED"
            : null;

        const insertRes = await client.query(
            `INSERT INTO payments (id, booking_id, status, amount, currency, reason)
             VALUES ($1, $2, $3, $4, $5, $6)
             ON CONFLICT (booking_id) DO NOTHING
             RETURNING id, booking_id, status, amount, currency, reason, created_at, updated_at`,
            [paymentId, command.bookingId, status, amount, currency, reason],
        );

        if (insertRes.rows.length === 0) {
            // A concurrent process inserted between the check and insert
            const conflictRes = await client.query(
                `SELECT id, booking_id, status, amount, currency, reason, created_at, updated_at
                 FROM payments
                 WHERE booking_id = $1`,
                [command.bookingId],
            );
            const existingPayment = rowToPaymentRecord(conflictRes.rows[0]);
            await client.query("COMMIT");

            if (existingPayment.status === "SUCCEEDED") {
                const event: PaymentSucceededEvent = {
                    messageId: randomUUID(),
                    bookingId: existingPayment.bookingId,
                    type: "PaymentSucceeded",
                    occurredAt: new Date().toISOString(),
                    payload: {
                        paymentId: existingPayment.id,
                        amount: existingPayment.amount,
                    },
                };
                return { payment: existingPayment, isDuplicate: true, event };
            } else {
                const event: PaymentFailedEvent = {
                    messageId: randomUUID(),
                    bookingId: existingPayment.bookingId,
                    type: "PaymentFailed",
                    occurredAt: new Date().toISOString(),
                    payload: {
                        reason: existingPayment.reason ?? "PAYMENT_DECLINED",
                    },
                };
                return { payment: existingPayment, isDuplicate: true, event };
            }
        }

        const newPayment = rowToPaymentRecord(insertRes.rows[0]);
        await client.query("COMMIT");

        if (status === "SUCCEEDED") {
            const event: PaymentSucceededEvent = {
                messageId: randomUUID(),
                bookingId: newPayment.bookingId,
                type: "PaymentSucceeded",
                occurredAt: new Date().toISOString(),
                payload: {
                    paymentId: newPayment.id,
                    amount: newPayment.amount,
                },
            };
            return { payment: newPayment, isDuplicate: false, event };
        } else {
            const event: PaymentFailedEvent = {
                messageId: randomUUID(),
                bookingId: newPayment.bookingId,
                type: "PaymentFailed",
                occurredAt: new Date().toISOString(),
                payload: {
                    reason: reason ?? "PAYMENT_DECLINED",
                },
            };
            return { payment: newPayment, isDuplicate: false, event };
        }
    } catch (error) {
        try {
            await client.query("ROLLBACK");
        } catch {
            // Preserve original error
        }
        throw error;
    } finally {
        client.release();
    }
}
