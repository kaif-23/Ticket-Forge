import { Redis, type RedisOptions } from "ioredis";

const REDIS_HOST = process.env.REDIS_HOST ?? "localhost";
const REDIS_PORT = Number(process.env.REDIS_PORT ?? 6379);

export function getRedisConfig(): RedisOptions {
    return {
        host: REDIS_HOST,
        port: REDIS_PORT,
        maxRetriesPerRequest: 1,
        connectTimeout: 1000,
        commandTimeout: 1000,
        retryStrategy(times) {
            if (times > 3) return null;
            return Math.min(times * 100, 1000);
        },
    };
}

let redisInstance: Redis | null = null;

export function getRedisClient(): Redis {
    if (!redisInstance) {
        redisInstance = new Redis(getRedisConfig());
        redisInstance.on("error", () => {
            // Gracefully handled by callers catching command errors
        });
    }
    return redisInstance;
}

export async function isRedisConnected(client = getRedisClient()): Promise<boolean> {
    try {
        const pong = await client.ping();
        return pong === "PONG";
    } catch {
        return false;
    }
}

export async function closeRedis(): Promise<void> {
    if (redisInstance) {
        try {
            await redisInstance.quit();
        } catch {
            redisInstance.disconnect();
        }
        redisInstance = null;
    }
}
