/**
 * A Topic consumer handles the actual reading of and ultimate streaming of a topic
 */

import { TopicMessageDocument, TopicMessageIdentifier } from "./types";


export abstract class TopicConsumer<T> {
    public topicName: string;

    protected messageCallback: (message: TopicMessageDocument<T>) => Promise<void>;
    protected noMoreMessagesCallback: () => void;
    protected crashCallback: (e: Error) => void;
    protected lastId: TopicMessageIdentifier | null;
    protected queryDoc: any;

    constructor(topicName: string) {
        this.topicName = topicName;

        this.messageCallback = async () => {};
        this.noMoreMessagesCallback = () => {};
        this.crashCallback = (e: Error) => {};

        this.lastId = null;
        this.queryDoc = {};
    }

    abstract start() : Promise<void>;

    abstract streamMessagesFrom(
        messageCallback: (message: TopicMessageDocument<T>) => Promise<void>, 
        lastId: null | TopicMessageIdentifier,
        noMoreMessagesCallback: () => void,
        crashCallback: () => void
    ): void;

    abstract stop() : Promise<void>;
}