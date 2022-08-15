# Streamable Topic

Event streaming concepts implemented on top of an abstracted storage backend.  

A topic is a list of ordered messages containing known payloads.  You may push new messages to the end of a topic, and consume messages starting from some position.  A consumer will keep track of the last message consumed and will always stream new messages as they arrive for you.  Messages are always guaranteed to be streamed in the correct order.

Produce messages with logCompactIds to notify the backend that any previous message with the same id is now redundant and can be removed from the storage engine automatically.

Use a Topic Setter in conjunction with one or many Topic Consumers to build a Topic Processor.  A processor is a recoverable process that transforms data from one topic to another.

Specify the Payload type when you create the Comsumer Producer instances.  Make sure that types are at least backwards compatible with existing data, otherwise you may accidently stream something with unexpected incorrect typings.  Use optional keys or a whole new version that the topic supports.

These concepts are relatively simple but a powerful building block for event based systems.

## Backend Implementations

Right now, I have exported a MongoRedis backend which uses Mongo collections for message storage and a shared Redis for signalling between consumers.  The Mongo collection name is the name of the topic and each document represents a message in that topic.  The producer for this backend will publish a redis message on the topic channel to notify when a new message has been added to storage.  This is used to wake up any listening consumers that are waiting for new messages on that topic.  This is a preferable method over having each consumer poll mongo for new messages every x seconds.

In the future I might add an Apache Kafka backend implementation.  Kafka already handles topic storage and streaming new messages out of the box without any need for polling mechanisms or signalling between producer and consumer.  In fact, the producer and consumer are purposely kept separated.

### MongoRedis Example

npx ts-node ./src/backends/mongoRedis/tests/testConsumer.ts 

npx ts-node ./src/backends/mongoRedis/tests/testProducer.ts 
