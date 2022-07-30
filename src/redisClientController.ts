
import Redis from "ioredis";

export class RedisClientController {

    private redisUrl: string;
    private connectionId: string;

    private client: null | Redis;
    private isStarting: boolean
    private isStarted: boolean
    
    private eventsCallback: null | ((status: "reconnecting" | "ready", connectionId: string) => void)

    
    constructor(redisUrl: string, connectionId: string) {
        this.redisUrl = redisUrl;
        this.connectionId = connectionId;

        this.client = null;
        this.isStarting = false;
        this.isStarted = false;

        this.eventsCallback = () => {};
    }

    getNewRedisClient() {
        const client = new Redis(this.redisUrl, { 
            lazyConnect: true,
            autoResubscribe: false,
            commandQueue: false,
            enableOfflineQueue: false,
            
            retryStrategy(times) {
                const delay = Math.min(times * 500, 5000);
                return delay;
            },
        });
        // console.log("Creating redis client: " + this.connectionId + "_" + name);
        return client;
    }

    async start(eventsCallback: null | ((status: "reconnecting" | "ready", connectionId: string) => void)) {
        if (this.isStarting) {
            return false; // Already started up by something else
        }
        this.isStarting = true;

        this.eventsCallback = eventsCallback;

        this.client = this.getNewRedisClient();
        this.client.on('error', (err: any) => {
            console.error("REDIS CLIENT " + this.connectionId + ": Error", err);
        });
        this.client.on('connect', () => {
            // console.log("REDIS CLIENT " + this.connectionId + ": Connected");
        });
        this.client.on('ready', () => {
            // console.log("REDIS CLIENT " + this.connectionId + ": Ready");
            if (this.eventsCallback !== null) {
                this.eventsCallback("ready", this.connectionId);
            }
        });
        this.client.on('close', () => {
            console.error("REDIS CLIENT " + this.connectionId + ": Close");
        });
        this.client.on('reconnecting', () => {
            console.warn("REDIS CLIENT " + this.connectionId + ": Reconnecting...");
            if (this.eventsCallback !== null) {
                this.eventsCallback("reconnecting", this.connectionId);
            }
        });

        await this.client.connect();
        this.isStarted = true;
    }

    getClient() : Redis {
        if (this.client === null) {
            throw new Error("You must startup this controller");
        } else {
            if (this.isStarted) {
                return this.client;
            } else {
                throw new Error("Redis client still starting up, please await the startup function properly");
            }
        }
    }

}