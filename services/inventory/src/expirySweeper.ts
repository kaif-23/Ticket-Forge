import { randomUUID } from "node:crypto";
import type { Producer } from "kafkajs";
import type { Pool } from "pg";
import type { HoldExpiredEvent } from "../../../packages/common/src/messages.js";
import { expireHeldSeats, type ExpiredBooking } from "./expireSeats.js";

export interface ExpirySweeperOptions {
    sweepIntervalMs?: number;
}

export class ExpirySweeper {
    private timer: NodeJS.Timeout | null = null;
    private isSweeping = false;
    private isRunning = false;
    private readonly sweepIntervalMs: number;

    constructor(
        private readonly pool: Pool,
        private readonly producer: Producer,
        options?: ExpirySweeperOptions,
    ) {
        this.sweepIntervalMs = options?.sweepIntervalMs ?? 1000;
    }

    start(): void {
        if (this.isRunning) return;
        this.isRunning = true;
        this.scheduleNext();
    }

    async stop(): Promise<void> {
        this.isRunning = false;
        if (this.timer) {
            clearTimeout(this.timer);
            this.timer = null;
        }

        while (this.isSweeping) {
            await new Promise((resolve) => setTimeout(resolve, 50));
        }
    }

    trigger(): void {
        if (!this.isRunning) return;
        if (this.timer) {
            clearTimeout(this.timer);
            this.timer = null;
        }
        void this.sweepAndReschedule();
    }

    async sweepOnce(): Promise<{
        expiredSeatsCount: number;
        expiredBookings: ExpiredBooking[];
    }> {
        const { expiredSeats, expiredBookings } = await expireHeldSeats();

        if (expiredBookings.length > 0) {
            for (const expired of expiredBookings) {
                const event: HoldExpiredEvent = {
                    messageId: randomUUID(),
                    bookingId: expired.bookingId,
                    type: "HoldExpired",
                    occurredAt: new Date().toISOString(),
                    payload: {
                        eventId: expired.eventId,
                        seatNos: expired.seatNos,
                    },
                };

                await this.producer.send({
                    topic: "inventory.events",
                    messages: [
                        {
                            key: expired.bookingId,
                            value: JSON.stringify(event),
                        },
                    ],
                });

                console.log("Published HoldExpired event:", {
                    bookingId: expired.bookingId,
                    eventId: expired.eventId,
                    seatNos: expired.seatNos,
                });
            }
        }

        return {
            expiredSeatsCount: expiredSeats.length,
            expiredBookings,
        };
    }

    private scheduleNext(): void {
        if (!this.isRunning) return;
        this.timer = setTimeout(() => {
            void this.sweepAndReschedule();
        }, this.sweepIntervalMs);
    }

    private async sweepAndReschedule(): Promise<void> {
        if (!this.isRunning || this.isSweeping) return;
        this.isSweeping = true;

        try {
            await this.sweepOnce();
        } catch (error) {
            console.error("Error during hold expiry sweep:", error);
        } finally {
            this.isSweeping = false;
            this.scheduleNext();
        }
    }
}
