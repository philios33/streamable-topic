/**
 * A Topic producer handles the actual writing of messages to a topic
 */

export abstract class TopicProducer<T> {
    public topicName: string;

    constructor(topicName: string) {
        this.topicName = topicName;
    }

    abstract start() : Promise<void>;

    abstract pushMessageToTopic(messagePayload: T, messageShardingKey: string, logCompactId?: string): Promise<void>

    abstract stop() : Promise<void>;
}
