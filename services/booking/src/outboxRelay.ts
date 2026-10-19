import type { Pool } from "pg";

export interface KafkaProducerLike {
    send(record: {
        topic: string;
        messages: Array<{ key?: string; value: string | Buffer | null }>;
    }): Promise<unknown>;
}

export interface OutboxRelayOptions {
    pollIntervalMs?: number;
    batchSize?: number;
}

export async function publishOutboxBatch(
    pool: Pool,
    producer: KafkaProducerLike,
    batchSize = 50,
): Promise<number> {
    const client = await pool.connect();
    try {
        await client.query("BEGIN");

        const result = await client.query<{
            id: string;
            booking_id: string;
            topic: string;
            type: string;
            occurred_at: Date | string;
            payload: Record<string, unknown>;
        }>(
            `SELECT id, booking_id, topic, type, occurred_at, payload
             FROM outbox
             WHERE published_at IS NULL
             ORDER BY created_at ASC
             LIMIT $1
             FOR UPDATE SKIP LOCKED`,
            [batchSize],
        );

        if (result.rows.length === 0) {
            await client.query("COMMIT");
            return 0;
        }

        const byTopic = new Map<
            string,
            Array<{ id: string; key: string; value: string }>
        >();

        for (const row of result.rows) {
            const occurredAt =
                row.occurred_at instanceof Date
                    ? row.occurred_at.toISOString()
                    : new Date(row.occurred_at).toISOString();

            const envelope = {
                messageId: row.id,
                bookingId: row.booking_id,
                type: row.type,
                occurredAt,
                payload: row.payload,
            };

            const entries = byTopic.get(row.topic) ?? [];
            entries.push({
                id: row.id,
                key: row.booking_id,
                value: JSON.stringify(envelope),
            });
            byTopic.set(row.topic, entries);
        }

        const publishedIds: string[] = [];

        for (const [topic, entries] of byTopic) {
            await producer.send({
                topic,
                messages: entries.map((e) => ({
                    key: e.key,
                    value: e.value,
                })),
            });

            for (const e of entries) {
                publishedIds.push(e.id);
            }
        }

        await client.query(
            `UPDATE outbox
             SET published_at = NOW()
             WHERE id = ANY($1)`,
            [publishedIds],
        );

        await client.query("COMMIT");
        return publishedIds.length;
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

export class OutboxRelay {
    private isRunning = false;
    private isPolling = false;
    private hasPendingTrigger = false;
    private timer: NodeJS.Timeout | null = null;
    private readonly pollIntervalMs: number;
    private readonly batchSize: number;

    constructor(
        private readonly pool: Pool,
        private readonly producer: KafkaProducerLike,
        options?: OutboxRelayOptions,
    ) {
        this.pollIntervalMs = options?.pollIntervalMs ?? 1000;
        this.batchSize = options?.batchSize ?? 50;
    }

    start(): void {
        if (this.isRunning) return;
        this.isRunning = true;
        this.schedulePoll(0);
    }

    async stop(): Promise<void> {
        this.isRunning = false;
        if (this.timer) {
            clearTimeout(this.timer);
            this.timer = null;
        }
        while (this.isPolling) {
            await new Promise((resolve) => setTimeout(resolve, 20));
        }
    }

    trigger(): void {
        if (!this.isRunning) return;
        if (this.isPolling) {
            this.hasPendingTrigger = true;
            return;
        }
        if (this.timer) {
            clearTimeout(this.timer);
            this.timer = null;
        }
        this.schedulePoll(0);
    }

    async pollOnce(): Promise<number> {
        return publishOutboxBatch(this.pool, this.producer, this.batchSize);
    }

    private schedulePoll(delayMs: number): void {
        if (!this.isRunning) return;
        this.timer = setTimeout(async () => {
            if (!this.isRunning) return;
            this.isPolling = true;
            try {
                const count = await this.pollOnce();
                const nextDelay = count >= this.batchSize ? 20 : this.pollIntervalMs;
                this.schedulePoll(nextDelay);
            } catch (error) {
                console.error("Outbox relay poll error; will retry on next poll tick:", error);
                this.schedulePoll(this.pollIntervalMs);
            } finally {
                this.isPolling = false;
                if (this.hasPendingTrigger && this.isRunning) {
                    this.hasPendingTrigger = false;
                    this.schedulePoll(0);
                }
            }
        }, delayMs);
    }
}
