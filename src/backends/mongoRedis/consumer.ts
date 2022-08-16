/**
 * This Mongo Redis Topic Consumer uses a mongo collection to persistenly represent a message stream.
 * Each document in the collection represents a message within the topic.
 * 
 * A redis connection is used to broadcast signals when new messages arrive to unlock waiting consumers.
 * This allows us to avoid a polling mechanism inside the consumer code.
 */

import Redis from "ioredis";
import { Document, MongoClient } from "mongodb";
import { TopicConsumer } from "../../topicConsumer";
import { MongoClientController } from "./mongoClientController";
import { RedisClientController } from "./redisClientController";
import { TopicMessageDocument, TopicMessageIdentifier, MongoTopicMessageDocument } from "../../types";


export class MongoRedisTopicConsumer<T> extends TopicConsumer<T> {

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

    
    private hasCalledNoMoreMessages: boolean;

    constructor(mongoUrl: string, databaseName: string, collectionName: string, redisUrl: string) {
        super(collectionName);

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
        
        this.hasCalledNoMoreMessages = false;
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

    public async streamMessagesFrom(callback: (message: TopicMessageDocument<T>) => void, lastId: null | TopicMessageIdentifier = null, queryDoc: Document = {}, noMoreMessages: () => void = () => {}): Promise<void> {
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
                if (!this.hasCalledNoMoreMessages) {
                    this.hasCalledNoMoreMessages = true;
                    try {
                        noMoreMessages();
                    } catch(e) {
                        // Ignore but display error
                        console.error(e);
                    }
                }
                return;
            }
            if (this.isPolling) {
                console.warn("Locked, still polling...");
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
        const newMessages = await this.fetchNextMessagesFromMongo(100, this.lastId, this.queryDoc);
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

                    // Convert to generic type of topic message with "id" field instead of "_id"
                    const topicMsg = { ...msg, _id: undefined, id: msg._id }
                    delete topicMsg._id;
                    this.callback(topicMsg as TopicMessageDocument<T>);
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

    private async fetchNextMessagesFromMongo(limit: number, lastId: TopicMessageIdentifier | null, queryDoc: any): Promise<Array<MongoTopicMessageDocument<T>>> {
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
         * It is important that (during writing) we generate a new _id which is incremented every time (auto incremented) so it is unique and retains order.
         * This means floods of messages arriving at once from multiple producers should be handled consistently by the system.
         * 
         * Note: Unsorted find order seems to be arbitrary depending on client, so we must explicitly sort by _id.
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
