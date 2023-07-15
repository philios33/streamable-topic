
// npx ts-node ./src/backends/mongoRedis/tests/testConsumer.ts 

import { MongoRedisTopicConsumer } from "../consumer";

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
        const tc = new MongoRedisTopicConsumer<PhilMessage>(mongoUrl, databaseName, collectionName, redisHost, redisPort);
        await tc.start();

        tc.streamMessagesFrom(async (msg) => {
            console.log("MSG: " + JSON.stringify(msg));

            /*
            if (msg.id > 20) {
                throw new Error("Simulate crash");
            }
            */

        }, null, () => {
            console.log("Queue is empty again");
        }, (error: Error) => {
            console.error("Consumer crashed: " + error.message);
        });

        setTimeout(async () => {
            await tc.stop();
            console.log("Stopped consuming after 5 minutes");
        }, 300 * 1000);
        
    } catch(e) {
        console.error(e);
        process.exit(1);
    }

})();