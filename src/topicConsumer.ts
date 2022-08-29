/**
 * A Topic consumer handles the actual reading of and ultimate streaming of a topic
 */

import { TopicMessageDocument, TopicMessageIdentifier } from "./types";


export abstract class TopicConsumer<T> {
    public topicName: string;

    protected callback: (message: TopicMessageDocument<T>) => void;
    protected lastId: TopicMessageIdentifier | null;
    protected queryDoc: any;

    constructor(topicName: string) {
        this.topicName = topicName;

        this.callback = () => {};
        this.lastId = null;
        this.queryDoc = {};
    }

    abstract start() : Promise<void>;

    abstract streamMessagesFrom(callback: (message: TopicMessageDocument<T>) => void, lastId: null | TopicMessageIdentifier, queryDoc: any, noMoreMessages: () => void): Promise<void>;

    abstract stop() : Promise<void>;
}