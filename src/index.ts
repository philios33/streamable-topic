export { TopicProducer } from './topicProducer';
export { TopicConsumer } from './topicConsumer';

// export { default as SoleTopicSetter } from "./OLD_soleTopicSetter";

export { MongoRedisTopicConsumer} from './backends/mongoRedis/consumer';
export { MongoRedisTopicProducer} from './backends/mongoRedis/producer';

export * from './types';