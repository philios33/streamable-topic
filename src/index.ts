import { TopicProducer as TopicProducerAlias } from "./topicProducer";
import { TopicConsumer as TopicConsumerAlias } from "./topicConsumer";

import TopicSetterAlias from "./topicSetter";

import { MongoRedisTopicConsumer as MongoConsumerAlias } from './backends/mongoRedis/consumer';
import { MongoRedisTopicProducer as MongoProducerAlias } from './backends/mongoRedis/producer';

export default {
    TopicProducer: TopicProducerAlias,
    TopicConsumer: TopicConsumerAlias,
    
    TopicSetter: TopicSetterAlias,

    // Bonus backends already implemented for you
    MongoRedisTopicConsumer: MongoConsumerAlias,
    MongoRedisTopicProducer: MongoProducerAlias,
}

export const TopicProducer = TopicProducerAlias;
export const TopicConsumer = TopicConsumerAlias;

export const TopicSetter = TopicSetterAlias;

export const MongoRedisTopicConsumer = MongoConsumerAlias;
export const MongoRedisTopicProducer = MongoProducerAlias;
