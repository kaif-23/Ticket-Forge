import { randomUUID } from "node:crypto";
import { Kafka, type EachMessagePayload } from "kafkajs";
import type { Pool } from "pg";
import { z } from "zod";
import type {
    PaymentFailedEvent,
    PaymentSucceededEvent,
    ProcessPaymentCommand,
} from "../../../packages/common/src/messages.js";
import { pool as defaultPool } from "./db.js";
import {
    executePayment,
    getPaymentByBookingId,
    type PaymentExecutionResult,
    type PaymentRecord,
    type ProcessPaymentInput,
} from "./payments.js";

export {
    defaultPool as pool,
    executePayment,
    getPaymentByBookingId,
    type PaymentExecutionResult,
    type PaymentRecord,
    type ProcessPaymentInput,
};

const processPaymentCommandSchema = z.object({
    messageId: z.string().min(1),
    bookingId: z.string().min(1),
    type: z.literal("ProcessPayment"),
    occurredAt: z.string().min(1),
    payload: z.object({
        amount: z.number(),
        currency: z.string().optional(),
        simulateFailure: z.boolean().optional(),
    }),
});

export function determinePaymentOutcome(
    command: ProcessPaymentCommand & { payload: { simulateFailure?: boolean } },
): PaymentSucceededEvent | PaymentFailedEvent {
    const occurredAt = new Date().toISOString();
    const shouldFail =
        command.payload.amount <= 0 ||
        command.payload.simulateFailure === true ||
        command.bookingId.toLowerCase().includes("fail");

    if (shouldFail) {
        return {
            messageId: randomUUID(),
            bookingId: command.bookingId,
            type: "PaymentFailed",
            occurredAt,
            payload: {
                reason: command.payload.amount <= 0 ? "INVALID_AMOUNT" : "PAYMENT_DECLINED",
            },
        };
    }

    return {
        messageId: randomUUID(),
        bookingId: command.bookingId,
        type: "PaymentSucceeded",
        occurredAt,
        payload: {
            paymentId: randomUUID(),
            amount: command.payload.amount,
        },
    };
}

export function createPaymentService(options?: {
    brokers?: string[];
    groupId?: string;
    clientId?: string;
    pool?: Pool;
}) {
    const paymentPool = options?.pool ?? defaultPool;
    const kafka = new Kafka({
        clientId: options?.clientId ?? "ticketforge-payment",
        brokers: options?.brokers ?? ["localhost:9092"],
    });

    const consumer = kafka.consumer({
        groupId: options?.groupId ?? "payment-service-group",
    });

    const producer = kafka.producer();

    async function start() {
        await producer.connect();
        await consumer.connect();

        await consumer.subscribe({
            topic: "payment.commands",
            fromBeginning: true,
        });

        console.log("Payment service listening for commands...");

        await consumer.run({
            eachMessage: async ({ topic, partition, message }: EachMessagePayload) => {
                const rawValue = message.value?.toString();
                if (!rawValue) return;

                let parsedJson: unknown;
                try {
                    parsedJson = JSON.parse(rawValue);
                } catch {
                    console.error("Invalid JSON received on payment.commands; skipping", {
                        topic,
                        partition,
                        offset: message.offset,
                    });
                    return;
                }

                const result = processPaymentCommandSchema.safeParse(parsedJson);
                if (!result.success) {
                    console.error("Invalid ProcessPayment command:", result.error.issues);
                    return;
                }

                const command = result.data as ProcessPaymentInput;
                const { payment, isDuplicate, event } = await executePayment(
                    paymentPool,
                    command,
                    options?.groupId ?? "payment-service-group",
                    topic,
                );

                if (isDuplicate) {
                    console.log("Duplicate ProcessPayment command received; skipping event emission", {
                        messageId: command.messageId,
                        bookingId: command.bookingId,
                    });
                    return;
                }

                await producer.send({
                    topic: "payment.events",
                    messages: [
                        {
                            key: event.bookingId,
                            value: JSON.stringify(event),
                        },
                    ],
                });

                console.log("Processed payment and published event:", {
                    bookingId: event.bookingId,
                    type: event.type,
                    isDuplicate,
                    paymentId: payment.id,
                    status: payment.status,
                });
            },
        });
    }

    async function stop() {
        await consumer.disconnect();
        await producer.disconnect();
    }

    return { start, stop, consumer, producer, pool: paymentPool };
}

// Auto-run when executed directly
if (process.argv[1] && process.argv[1].replace(/\\/g, "/").endsWith("services/payment/src/index.ts")) {
    const service = createPaymentService();
    service.start().catch((err) => {
        console.error("Payment Service startup failure:", err);
        process.exit(1);
    });

    const shutdown = async (signal: string) => {
        console.log(`Received ${signal}, stopping payment service...`);
        await service.stop();
        process.exit(0);
    };

    process.on("SIGINT", () => void shutdown("SIGINT"));
    process.on("SIGTERM", () => void shutdown("SIGTERM"));
}
