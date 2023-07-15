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
import { ReliableRedisClient } from "./reliableRedisClient";
import { TopicMessageDocument, TopicMessageIdentifier, MongoTopicMessageDocument } from "../../types";


export class MongoRedisTopicConsumer<T> extends TopicConsumer<T> {

    private mongoUrl: string;
    private databaseName: string;
    private collectionName: string;
    private redisHost: string;
    private redisPort: number;

    private client: MongoClient | null;
    private redis: Redis | null;
    private isStarting: boolean;
    private isStreaming: boolean;
    private isPolling: boolean;
    private isMoreMessages: boolean;
    private isStopped: boolean;

    private debuggingHandlers: Array<(type: string, message: string) => void>
    private crashedHandlers: Array<(error: Error) => void>
    
    private hasCalledNoMoreMessages: boolean;

    private pollingLoop: null | ReturnType<typeof setInterval>;

    constructor(mongoUrl: string, databaseName: string, collectionName: string, redisHost: string, redisPort: number) {
        super(collectionName);

        this.client = null;
        this.redis = null;
        // console.log("Constructing a new Consuer, redis is null");
        this.mongoUrl = mongoUrl;
        this.databaseName = databaseName;
        this.collectionName = collectionName;
        this.isStarting = false;
        this.isStreaming = false;
        this.isPolling = false;
        this.isMoreMessages = true;
        this.isStopped = false;
        this.redisHost = redisHost;
        this.redisPort = redisPort;
        
        this.hasCalledNoMoreMessages = false;
        this.debuggingHandlers = [];
        this.crashedHandlers = [];

        this.pollingLoop = null;
    }

    public async start(): Promise<void> {
        if (this.isStarting) {
            throw new Error("Already started");
        }
        this.fireDebug("START", "this.started was called on the consumer");
        this.isStarting = true;
        const mcc = new MongoClientController(this.mongoUrl, this.collectionName + "-consumer");
        await mcc.start();
        this.client = mcc.getClient();
        // console.log("Mongo client is connected and ready!");

        const rrc = new ReliableRedisClient(this.collectionName + "-consumer", this.redisHost, this.redisPort);
        await rrc.start(async (event) => {
            if (event.type === "FIRST_READY" || event.type === "RECONNECTED") {
                // Resubscribe
                if (this.redis !== null) {
                    await this.startListeningForNewMessageSignal();
                    // console.log("Resubscribed to redis channel!");
                } else {
                    // This callback occurred during the startup process, which means it is the very first ready event and start listening soon anyway
                }
                
                // Make sure we trigger the poller when redis reconnects
                this.isMoreMessages = true;
            }
        });
        this.redis = rrc.getClient();
        // console.log("Redis client is connected and ready");

        // Handle wake signal
        this.redis.on("message", (channel, message) => {
            this.isMoreMessages = true;
            this.fireDebug("REDIS_TRIGGER", "The consumer received a trigger from redis to check the mongo DB");
        });
        
        await this.startListeningForNewMessageSignal();
        // console.log("Subscribed to redis signal channel!");

        this.fireDebug("START_FINISHED", "this.started finished on the consumer");
    }

    public async stop() {       
        this.fireDebug("STOPPED", "The consumer has unsubscribed and will quit the polling loop");
        if (this.pollingLoop !== null) {
            clearInterval(this.pollingLoop);
            this.pollingLoop = null;
        }
        
        this.isStopped = true;
        
        this.client?.close();
        this.redis?.disconnect(false);
    }

    /*
    public addCrashedHandler(handler: (error: Error) => void) {
        this.crashedHandlers.push(handler);
    }
    */

    public addDebuggingHandler(handler: (type: string, message: string) => void) {
        this.debuggingHandlers.push(handler);
    }

    /*
    private fireCrashedHandlers(error: Error) {
        if (this.isStopped) {
            return;
        }
        for (const handler of this.crashedHandlers) {
            try {
                handler.apply(this, [error]);
            } catch(e) {
                console.error("ERROR: Crashed handler threw exception, please handle this");
                console.error(e);
            }
        }
    }
    */

    private fireDebug(type: string, message: string) {
        if (this.isStopped) {
            return;
        }
        for (const handler of this.debuggingHandlers) {
            try {
                console.log(type, message);
                handler.apply(this, [type, message]);
            } catch(e) {
                console.error("ERROR: Debugging handler threw exception, please handle this");
                console.error(e);
            }
        }
    }

    private getCollection() {
        if (this.client === null) {
            throw new Error("this.client is null");
        }
        return this.client.db(this.databaseName).collection(this.collectionName);
    }

    // Sets up a consuming stream.  
    // The messageCallback is called for every message that is streamed and is expected to be handled properly.
    // The noMoreMessagesCallback is called whenever the queue is exhausted
    // The crashCallback is called when the consumer crashes.
    public streamMessagesFrom(messageCallback: (message: TopicMessageDocument<T>) => Promise<void>, lastId: null | TopicMessageIdentifier = null, noMoreMessagesCallback: () => void, crashCallback: (e: Error) => void): void {
        if (!this.isStarting) {
            throw new Error("Cannot stream on a non started consumer");
        }
        if (this.isStreaming) {
            throw new Error("Already streaming");
        }
        this.isStreaming = true;

        this.messageCallback = messageCallback;
        this.noMoreMessagesCallback = noMoreMessagesCallback;
        this.crashCallback = crashCallback;

        this.lastId = lastId;

        // Start a loop here which polls for new messages until there are no more
        this.pollingLoop = setInterval(async () => {
            // Stop forever
            if (this.isStopped) {
                this.fireDebug("STOPPED", "The consumer has unsubscribed and will quit the polling loop");
                if (this.pollingLoop !== null) {
                    clearInterval(this.pollingLoop);
                    this.pollingLoop = null;
                }
                return;
            }
            if (!this.isMoreMessages) { // Set to true to unlock the interval
                // Calling a noMoreMessages function is useful for the system to know when all processing is complete
                if (!this.hasCalledNoMoreMessages) {
                    this.hasCalledNoMoreMessages = true;
                    try {
                        this.fireDebug("NO_MORE_MESSAGES", "There are no more messages (for now)");
                        this.noMoreMessagesCallback();
                    } catch(e) {
                        // Ignore but display error
                        console.error(e);
                    }
                }
                return;
            }
            if (this.isPolling) {
                this.fireDebug("LOCKED", "The polling loop is still running, so we do nothing for this execution");
                return;
            }
            this.isPolling = true;
            try {
                this.fireDebug("POLLING_STARTED", "The consumer is running its polling routine");
                await this.pollForNewMessages();
                this.fireDebug("POLLING_FINISHED", "The consumer finished its polling routine");
                this.isPolling = false;
            } catch(e) {
                this.fireDebug("POLLING_FAILED", "The consumer failed to run its polling routine");
                this.isPolling = false;
            }
        }, 1000);

        

    }

    private async pollForNewMessages() {
        // console.log("Polling for new messages");
        const newMessages = await this.fetchNextMessagesFromMongo(100, this.lastId);
        if (newMessages.length === 0) {
            // No more messages
            // console.log("No more messages");
            this.isMoreMessages = false;
        } else {
            this.fireDebug("FOUND", "Found " + newMessages.length + " new messages to process");
            // Callback these messages
            for (const msg of newMessages) {
                // Note: If we don't catch this here, it means that polling will fail if the callback throws an error.  
                // The poll will instantly retry anyway which will cause the consumer to be spammed with the same next message.
                // To avoid this behaviour, we say that the consumer crashed and runs the crashed callback routine.
                try {
                    this.lastId = msg._id;

                    this.fireDebug("MESSAGE", "Processing message: " + msg._id + " (" + msg.logCompactId + ") pushed at " + msg.createdAt);

                    // Convert to generic type of topic message with "id" field instead of "_id"
                    const topicMsg = { ...msg, _id: undefined, id: msg._id }
                    delete topicMsg._id;
                    this.messageCallback(topicMsg as TopicMessageDocument<T>);
                    this.fireDebug("CONSUMED", "The callback has finished processing the message");
                } catch(e: any) {
                    this.fireDebug("CONSUMPTION_FAILED", "The callback failed to process the message properly: " + e.message);
                    console.error(e);
                    console.error("The consumer could not handle the message", JSON.stringify(msg, null, 4));
                    console.error("The consumer will not stream any more messages");
                    try {
                        this.crashCallback(e);
                    } catch(e: any) {
                        console.error("Crash callback crashed, please fix this first");
                        console.error(e);
                    }

                    this.stop();  // This will shutdown the whole consumer, removing timeouts and setting isStopped
                    
                    return;
                }                
            }
        }
    }

    private async fetchNextMessagesFromMongo(limit: number, lastId: TopicMessageIdentifier | null): Promise<Array<MongoTopicMessageDocument<T>>> {
        const col = this.getCollection();

        const thisQuery: any = {};
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
        this.fireDebug("REDIS_LISTENING", "The consumer is listening for topic signals using a redis subscription on channel: " + "TOPIC-" + this.collectionName);
    }

}
