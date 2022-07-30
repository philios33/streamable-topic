
/**
 * A Mongo Topic uses mongo collection to persistenly represent a message stream.
 * Each document in the collection represents a message within the stream/topic.
 * 
 * This is the consumer, it can stream all messages back in order, or from a point using some messages ObjectId at _id key
 */


import Redis from "ioredis";
import { Document, MongoClient } from "mongodb";
import { MongoClientController } from "./mongoClientController";
import { RedisClientController } from "./redisClientController";
import { MongoTopicMessageDocument } from "./types";

export class TopicConsumer<T> {

    private mongoUrl: string;
    private databaseName: string;
    private collectionName: string;
    private redisUrl: string;

    private client: MongoClient | null;
    private redis: Redis | null;
    private isStarting: boolean;
    private isStreaming: boolean;
    private isPolling: boolean;
    private isMoreMessages: boolean;
    private isCrashed: boolean;

    private callback: (message: MongoTopicMessageDocument<T>) => void;
    private lastId: number | null;
    private queryDoc: Document;

    constructor(mongoUrl: string, databaseName: string, collectionName: string, redisUrl: string) {       
        this.client = null;
        this.redis = null;
        this.mongoUrl = mongoUrl;
        this.databaseName = databaseName;
        this.collectionName = collectionName;
        this.isStarting = false;
        this.isStreaming = false;
        this.isPolling = false;
        this.isMoreMessages = true;
        this.isCrashed = false;
        this.redisUrl = redisUrl;
        this.callback = () => {};
        this.lastId = null;
        this.queryDoc = {};
    }

    async start(): Promise<void> {
        if (this.isStarting) {
            throw new Error("Already started");
        }
        this.isStarting = true;
        const mcc = new MongoClientController(this.mongoUrl, this.collectionName + "-consumer");
        await mcc.start();
        this.client = mcc.getClient();
        // console.log("Mongo client is connected and ready!");

        const rcc = new RedisClientController(this.redisUrl, this.collectionName + "-consumer");
        await rcc.start(async (connectionState) => {
            if (connectionState === "ready") {
                // Resubscribe
                if (this.redis !== null) {
                    await this.startListeningForNewMessageSignal();
                    // console.log("Resubscribed to redis channel!");
                }
                
                // Make sure we trigger the poller when redis reconnects
                this.isMoreMessages = true;
            }
        });
        this.redis = rcc.getClient();
        // console.log("Redis client is connected and ready !");

        // Handle wake signal
        this.redis.on("message", (channel, message) => {
            this.isMoreMessages = true;
        });
        
        await this.startListeningForNewMessageSignal();
        // console.log("Subscribed to redis signal channel!");
    }

    private getCollection() {
        if (this.client === null) {
            throw new Error("this.client is null");
        }
        return this.client.db(this.databaseName).collection(this.collectionName);
    }

    public async streamMessagesFrom(callback: (message: MongoTopicMessageDocument<T>) => void, lastId: null | number, queryDoc: Document): Promise<void> {
        if (this.isStreaming) {
            throw new Error("Already streaming");
        }
        this.isStreaming = true;

        this.callback = callback;
        this.lastId = lastId;
        this.queryDoc = queryDoc;

        // Start a loop here which polls for new messages until there are no more
        const pollingLoop = setInterval(async () => {
            // Stop forever
            if (this.isCrashed) {
                clearInterval(pollingLoop);
                return;
            }
            if (!this.isMoreMessages) { // Set to true to unlock the interval
                return;
            }
            if (this.isPolling) {
                return;
            }
            this.isPolling = true;
            try {
                await this.pollForNewMessages();
                this.isPolling = false;
            } catch(e) {
                console.warn(e);
                this.isPolling = false;
            }
        }, 1000);
    }

    private async pollForNewMessages() {
        // console.log("Polling for new messages");
        const newMessages = await this.fetchNextMessagesFromMongo(10, this.lastId, this.queryDoc);
        if (newMessages.length === 0) {
            // No more messages
            // console.log("No more messages");
            this.isMoreMessages = false;
        } else {
            // Callback these messages
            for (const msg of newMessages) {
                // Note: If we don't catch this here, it means that polling will fail if the callback throws an error.  
                // The poll will instantly retry anyway which will cause the consumer to be spammed with the same next message.
                // To avoid this behaviour, we say that the consumer itself fails and crash the whole instance.
                try {
                    this.lastId = msg._id;
                    this.callback(msg);
                } catch(e) {
                    console.error(e);
                    console.error("The consumer could not handle the message", JSON.stringify(msg, null, 4));
                    console.error("The consumer will not stream any more messages");
                    this.isCrashed = true;
                    return;
                }                
            }
        }
    }

    private async fetchNextMessagesFromMongo(limit: number, lastId: number | null, queryDoc: Document): Promise<Array<MongoTopicMessageDocument<T>>> {
        const col = this.getCollection();

        const thisQuery: any = {};
        for (const key in queryDoc) {
            thisQuery["payload." + key] = queryDoc[key];
        }
        if (lastId !== null) {
            thisQuery["_id"] = {
                $gt: lastId
            }
        }

        /** 
         * A note about message order.
         * Message order is very important within the realm of event based systems.  
         * There is only one order that messages can arrive in, and so there should only be one order that messages are streamed in.
         * Due to the single node mongo requirement, and the fact that it is mongo that creates the _id field for us,
         * Mongo will always use an incrementing ObjectId since the instanceId is constant.  This is something this system is designed to harness.
         * Even floods of messages arriving at once from multiple producers should be handled consistently by the system.
         * 
         * Update: No, this was not true in production.  Unsorted find order seems to be arbitrary depending on client.  This means we MUST have a proper auto incrementing field.
         **/
        const results = await col.find<MongoTopicMessageDocument<T>>(thisQuery).limit(limit).sort({
            _id: 1,
        }).toArray();
        return results;
    }

    private async startListeningForNewMessageSignal() {
        if (this.redis === null) {
            throw new Error("this.redis is null");
        }
        await this.redis.subscribe("TOPIC-" + this.collectionName);
    }

}
