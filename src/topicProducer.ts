/**
 * A Topic consumer handles the actual reading of and ultimate streaming of a topic
 */

export abstract class TopicProducer<T> {
    public topicName: string;

    constructor(topicName: string) {
        this.topicName = topicName;
    }

    abstract start() : Promise<void>;

    abstract pushMessageToTopic(messagePayload: T, logCompactId?: string): Promise<void>
}
