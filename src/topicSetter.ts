/**
 * Can't think of a better name for this right now but...
 * A Topic Setter is a layer between producing a message that sets some data, but ignores the update if there is no change.
 * First it consumes all messages in the topic and stores them by logCompactId
 * Then when we want to set the same id to the same payload value, it ignores it to prevent unnecessary messages.
 * This class is very useful in a processor that outputs to a log compacted topic.
 */

import { TopicConsumer } from "./topicConsumer";
import { TopicProducer } from "./topicProducer";
import { TopicMessageDocument } from "./types";
import objHash from "object-hash";

// We MUST replace the Date object with it's ISO string counterpart since the objects get serialized and deserialized 
// in to different Dates even though they are the same value.  This essentially overrides how Date equality is compared.
const hash = (item: any) => {
    return objHash(item, {
        replacer: (i) => {
            if (typeof i === "object" && i instanceof Date) {
                return i.toISOString();
            }
            return i;
        }
    });
}

type NewCompactedMessage<T> = {
    payload: T
    logCompactId: string
    queuedAt: Date
}

export default class TopicSetter<T> {
    private consumer: TopicConsumer<T>;
    private producer: TopicProducer<T>;

    private memoryHash: Record<string, string>;
    private isReady: boolean;

    private messagesToApply: Record<string, NewCompactedMessage<T>>;
    private messagesToPush: Array<T>;
    private isProducingMessages: boolean;
    private lastTriggered: Date;
    
    constructor(consumer: TopicConsumer<T>, producer: TopicProducer<T>) {
        this.consumer = consumer;
        this.producer = producer;
        this.memoryHash = {};
        this.isReady = false; // Set to false until the topic is completely loaded

        this.messagesToApply = {};
        this.messagesToPush = [];
        this.isProducingMessages = false;
        this.lastTriggered = new Date();
    }

    async start() {
        await this.consumer.start();
        await this.producer.start();

        await new Promise<void>((resolve, reject) => {
            this.consumer.streamMessagesFrom((msg) => this.processMessage(msg), null, {}, () => {
                // This will run after the final message has been processed
                // We know we are up to date
                this.isReady = true;
                resolve();
            });
            // TODO Perhaps reject after timeout, or warn if this takes ages and the promise never resolves!
        });

        // Begin interval loop that does the actual pushing to output topics every 60 seconds, or whenever the queue is triggered!
        setInterval(() => {
            this.triggerWaitingMessages();
        }, 60 * 1000);
    }

    public async triggerWaitingMessages() {
        const thisTriggered = new Date();
        if (this.isProducingMessages) {
            this.lastTriggered = thisTriggered;
            return;
        } else {
            this.isProducingMessages = true;
            try {
                await this.produceWaitingMessages();
                this.isProducingMessages = false;

                // Instantly retrigger if something has retriggered it during another trigger running
                // This prevents messages getting stuck until the next cycle if multiple things trigger the queue at once
                if (this.lastTriggered > thisTriggered) {
                    this.triggerWaitingMessages();
                }
            } catch(e) {
                const totalNum = this.messagesToPush.length + Object.keys(this.messagesToApply).length;
                console.warn("Topic setter failed to trigger waiting messages, will keep retrying, queue size is: " + totalNum);
                setTimeout(() => {
                    // This is really bad, we accepted a new message synchronously, but when the time came to send it, some error happened.
                    // Due to the fact that messages should retain order across a topic, we cannot continue, but we can catch failed sends and retry forever.
                    // Unlock after 20 seconds so it retries again
                    this.isProducingMessages = false;    
                }, 20 * 1000);
            }
        }
    }

    private async produceWaitingMessages() {
        const totalNum = this.messagesToPush.length + Object.keys(this.messagesToApply).length;
        
        // First push the non log compacted messages that must always be appended
        while (this.messagesToPush.length > 0) {
            const next = this.messagesToPush.shift();
            if (next) {
                try {
                    await this.producer.pushMessageToTopic(next);
                } catch(e: any) {
                    // This is probably an issue with backend connection, but either the next id or the write failed for some reason.
                    // We can safely unshift this message for the future back to the queue
                    this.messagesToPush.unshift(next);
                    throw new Error("Failed to produce message to topic: " + e.message);
                }
            }
        }

        // Then process the other messages by sending them.
        // Note: We await for confirmation of send
        // But we do not need to wait for hash confirmation here or measure any round trip time
        const newMessages = Object.values(this.messagesToApply);
        newMessages.sort((a, b) => {
            return a.queuedAt.getTime() - b.queuedAt.getTime();
        });
        while (newMessages.length > 0) {
            const next = newMessages.shift();
            if (next) {
                try {
                    await this.producer.pushMessageToTopic(next.payload, next.logCompactId);

                    // We didn't actually shift it out from this.messagesToApply yet
                    delete this.messagesToApply[next.logCompactId];
                } catch(e: any) {
                    // This is probably an issue with backend connection, but either the next id or the write failed for some reason.
                    // We don't need to unshift anything, the item is still in the Record
                    throw new Error("Failed to produce message to topic: " + e.message);
                }
            }
        }
        if (totalNum > 0) {
            console.log(totalNum + " messages were flushed");
        }
    }

    private processMessage(msg: TopicMessageDocument<T>) {
        if (msg.logCompactId) {
            this.memoryHash[msg.logCompactId] = hash(msg.payload);
        } else {
            console.warn("Warning, message " + msg.id + " in " + this.consumer.topicName + " has no logCompactId");
        }
    }

    public setLogCompactedPayload(logCompactId: string, payload: T) {
        if (!this.isReady) {
            throw new Error("Please await this.start properly, the topic setter isn't ready");
        }
        if (logCompactId in this.memoryHash) {
            const currentPayloadHash = this.memoryHash[logCompactId];
            const newPayloadHash = hash(payload);
            if (newPayloadHash === currentPayloadHash) {
                // Same data, ignore
                return;
            } else {
                // The data has changed, push a data update to the memory store to be executed asynchronously
            }
        } else {
            // Does not exist yet in memory, push a data update to memory
        }

        // The latest one will always overwrite one that hasn't been written yet
        this.messagesToApply[logCompactId] = {
            queuedAt: new Date(),
            payload,
            logCompactId,
        }

        // Note: This function is deliberately synchronous.  The producer interval makes the actual write attempts asynchronously
    }

    public setPayload(payload: T) {
        if (!this.isReady) {
            throw new Error("Please await this.start properly, the topic setter isn't ready");
        }
        
        // It doesn't matter what we have in memory, we just want to push this message always.
        this.messagesToPush.push(payload);

        // Note: This function is deliberately synchronous.  The producer interval makes the actual write attempts asynchronously
    }


}
