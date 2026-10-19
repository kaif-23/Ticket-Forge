import type { Pool, PoolClient } from "pg";

export async function recordProcessedMessage(
    clientOrPool: PoolClient | Pool,
    messageId: string,
    consumerGroup: string,
    topic: string,
): Promise<{ processed: boolean; isDuplicate: boolean }> {
    const result = await clientOrPool.query(
        `INSERT INTO processed_messages (message_id, consumer_group, topic, processed_at)
         VALUES ($1, $2, $3, NOW())
         ON CONFLICT (message_id, consumer_group) DO NOTHING
         RETURNING message_id`,
        [messageId, consumerGroup, topic],
    );

    if (result.rowCount === 0) {
        return { processed: false, isDuplicate: true };
    }
    return { processed: true, isDuplicate: false };
}

export async function isMessageProcessed(
    clientOrPool: PoolClient | Pool,
    messageId: string,
    consumerGroup: string,
): Promise<boolean> {
    const res = await clientOrPool.query(
        "SELECT 1 FROM processed_messages WHERE message_id = $1 AND consumer_group = $2",
        [messageId, consumerGroup],
    );
    return res.rows.length > 0;
}
