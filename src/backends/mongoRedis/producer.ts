/**
 * The Mongo Redis Topic Producer pushes a message to the end of the topic stream by 
 * fetching a new id, 
 * writing the new message document 
 * & notifying redis subscribers.
 */


import Redis from "ioredis";
import { Document, MongoClient } from "mongodb";
import { MongoClientController } from "./mongoClientController";
import { RedisClientController } from "./redisClientController";
import { TopicProducer } from "../../topicProducer";
import { MongoTopicMessageDocument, MongoSequenceDocument } from "./types";


export class MongoRedisTopicProducer<T> extends TopicProducer<T> {

    private mongoUrl: string;
    private databaseName: string;
    private collectionName: string;
    private redisUrl: string;

    private client: MongoClient | null;
    private redis: Redis | null;
    private isStarting: boolean;

    constructor(mongoUrl: string, databaseName: string, collectionName: string, redisUrl: string) {
        super(collectionName);

        this.client = null;
        this.redis = null;
        this.mongoUrl = mongoUrl;
        this.databaseName = databaseName;
        this.collectionName = collectionName;
        this.isStarting = false;
        this.redisUrl = redisUrl;
    }

    async start(): Promise<void> {
        if (this.isStarting) {
            throw new Error("Already started");
        }
        this.isStarting = true;
        const mcc = new MongoClientController(this.mongoUrl, this.collectionName + "-producer");
        await mcc.start();
        this.client = mcc.getClient();
        // console.log("Mongo client is connected and ready!");

        const rcc = new RedisClientController(this.redisUrl, this.collectionName + "-producer");
        await rcc.start(null);
        this.redis = rcc.getClient();
        // console.log("Redis client is connected and ready!");
    }
    

    private getCollection() {
        if (this.client === null) {
            throw new Error("this.client is null");
        }
        return this.client.db(this.databaseName).collection(this.collectionName);
    }

    private async getNextId() : Promise<number> {
        if (this.client === null) {
            throw new Error("this.client is null");
        }

        const result = await this.client.db(this.databaseName).collection<MongoSequenceDocument>("nextids").findOneAndUpdate({
            "_id": this.collectionName
        }, {
            $inc: {
                "seq":1
            },
        }, {
            returnDocument: "after",
            upsert: true
        });
        if (result.ok) {
            if (result.value === null) {
                throw new Error("None found but we have upsert enabled");
            }
            return result.value.seq;
        } else {
            console.error(result);
            throw new Error("Result not ok from getNextId");
        }
    }

    public async pushMessageToTopic(messagePayload: T, logCompactId?: string): Promise<void> {
        const col = this.getCollection();

        /**
         * A note on _id field
         * After extensive testing with the mongo.ObjectId type, it is obvious that it does NOT meet the needs of retaining document order, only uniqueness.
         * The only way to produce truly ordered messages (during concurrent producers running) is to obtain the next id field first from a lock collection, 
         * and do a proper sort by the _id field.
         * This makes the writes slower but will ensure proper message ordering.
         * 
         * Update: This also has the added benefit that we no longer need to require mongo running in single node mode.
         */
        const nextId = await this.getNextId();
        const msgDoc: MongoTopicMessageDocument<T> = {
            _id: nextId,
            createdAt: new Date(),
            payload: messagePayload,
        }
        if (logCompactId) {
            msgDoc.logCompactId = logCompactId;
        }

        const result = await col.insertOne(msgDoc as Document);
        if (!result.acknowledged) {
            throw new Error("Not acked");
        }

        // The write is confirmed by mongo so we can return here, but before we do, publish the message to redis for any listening consumers.
        // There is no guarantee that messages will end up being published to redis in the correct order, and order is important.
        // This just sends a signal to listening consumers that there is a new message written.  
        // All consumers should wake up and do a poll to fetch it.
        // Note: No need to await for this to return since it is not required for confirmation.
        this.broadcastNewMessageSignalToRedis();
        
        // TODO Handle log compacting by removing messages with matching logCompactId fields
        // TODO Write library to setup mongo indexes so that a topic collection has the correct index on the log compact field.
    }

    private async broadcastNewMessageSignalToRedis() {
        if (this.redis === null) {
            throw new Error("this.redis is null");
        }
        try {
            await this.redis.publish("TOPIC-" + this.collectionName, JSON.stringify({newMessage: true}));
            // console.log("Broadcasted a message to redis");
        } catch(e: any) {
            // Something went wrong with the wakeup signal
            console.warn("Warning, redis signal failed: " + e.message);
            console.warn(e);
            // We MUST continuously retry publishing this signal because consumers will never wake up otherwise
            // Retry in 10 seconds
            setTimeout(() => {
                this.broadcastNewMessageSignalToRedis();
            }, 10 * 1000);
        }
    }

}