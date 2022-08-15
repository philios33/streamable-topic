import { TopicProducer } from "./topicProducer";
import { TopicConsumer } from "./topicConsumer";
import TopicSetter from "./topicSetter";

import { MongoRedisTopicConsumer } from './backends/mongoRedis/consumer';
import { MongoRedisTopicProducer } from './backends/mongoRedis/producer';

export default {
    TopicProducer,
    TopicConsumer,
    
    TopicSetter,

    // Bonus backends already implemented for you
    MongoRedisTopicConsumer,
    MongoRedisTopicProducer,
}