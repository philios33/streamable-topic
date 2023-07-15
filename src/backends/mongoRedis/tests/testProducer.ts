
// npx ts-node ./src/backends/mongoRedis/tests/testProducer.ts 

import { MongoRedisTopicProducer } from "../producer";

const mongoUrl = "mongodb://localhost:27017";
const databaseName = "topics";
const collectionName = "phil2";
const redisHost = "localhost";
const redisPort = 6379;

type PhilMessage = {
    phil: string
    isGreat: boolean
}

(async () => {
    try {
        const tp = new MongoRedisTopicProducer<PhilMessage>(mongoUrl, databaseName, collectionName, redisHost, redisPort);
        const randomId = Math.random();
        await tp.start();
        for (let i=1; i<=30; i++) {
            await tp.pushMessageToTopic({
                phil: randomId + " message " + i,
                isGreat: i > 5,
            }, "shardingKeyHere");
            console.log("Successfully pushed message: " + i);
        }
        process.exit(0);
    } catch(e) {
        console.error(e);
        process.exit(1);
    }

})();