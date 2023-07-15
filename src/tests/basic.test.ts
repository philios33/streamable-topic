// This tests the basic consumer and producer behaviour
import { afterAll, beforeAll, describe, expect, jest, test } from '@jest/globals';
import { MongoRedisTopicConsumer } from "../backends/mongoRedis/consumer";
import { MongoRedisTopicProducer } from "../backends/mongoRedis/producer";

const mongoUrl = "mongodb://localhost:27017";
const databaseName = "topics-test";
const redisHost = "localhost";
const redisPort = 6379;

type ExampleMessage = {
    phil: string
    isGreat: boolean
}

const randomId = Math.round(Math.random() * 100000);
const collectionName = "example-" + randomId;
const tc = new MongoRedisTopicConsumer<ExampleMessage>(mongoUrl, databaseName, collectionName, redisHost, redisPort);
const tp = new MongoRedisTopicProducer<ExampleMessage>(mongoUrl, databaseName, collectionName, redisHost, redisPort);

const waitSecs = async (secs: number) => {
    return new Promise((resolve, reject) => {
        setTimeout(() => {
            resolve(null);
        }, secs * 1000);
    })
}

beforeAll(async () => {
    await tc.start();
    await tp.start();    
});

afterAll(async () => {
    await tc.stop();
    await tp.stop();
});

describe("Basic operations", () => {
    let foundMessages = 0;
    test("Should consume nothing at start", async () => {

        // Consumer
        await new Promise((resolve, reject) => {
            tc.streamMessagesFrom(async (msg) => {
                console.log("MSG: " + JSON.stringify(msg));
                foundMessages++;
            }, null, () => {
                // All messages done
                resolve(null);  
            }, (e) => {
                // Crashed
                reject(e);
            });
        });

        await waitSecs(5);
        expect(foundMessages).toEqual(0);
    
        // Producer
        for (let i=1; i<=10; i++) {
            await tp.pushMessageToTopic({
                phil: randomId + " message " + i,
                isGreat: i > 5,
            }, "shardingKey");
            console.log("Successfully pushed message: " + i);
        }

        await waitSecs(5);
        expect(foundMessages).toEqual(10);
    })
});
