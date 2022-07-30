import { TopicProducer } from "../topicProducer";


const mongoUrl = "mongodb://localhost:27017";
const databaseName = "topics";
const collectionName = "phil2";
const redisUrl = "redis://localhost:6379";

type PhilMessage = {
    phil: string
    isGreat: boolean
}

(async () => {
    try {
        const tp = new TopicProducer<PhilMessage>(mongoUrl, databaseName, collectionName, redisUrl);
        const randomId = Math.random();
        await tp.start();
        for (let i=1; i<=200; i++) {
            await tp.pushMessageToTopic({
                createdAt: new Date,
                payload: {
                    phil: randomId + " message " + i,
                    isGreat: i > 5,
                }
            });
            console.log("Successfully pushed message: " + i);
        }
        process.exit(0);
    } catch(e) {
        console.error(e);
        process.exit(1);
    }

})();