export * from './backends/mongoRedis/types';

export type TopicMessage<T> = {
    createdAt: Date
    shardingKey: string
    logCompactId?: string
    payload: T
}
export type TopicMessageIdentifier = number | string;

export type TopicMessageDocument<T> = TopicMessage<T> & {
    id: TopicMessageIdentifier // Depends on the backend implementation
}
