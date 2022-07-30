import { TopicConsumer } from "../topicConsumer";


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
        const tc = new TopicConsumer<PhilMessage>(mongoUrl, databaseName, collectionName, redisUrl);
        await tc.start();

        tc.streamMessagesFrom((msg) => {
            console.log("MSG: " + JSON.stringify(msg));
            if (msg._id > 20) {
                throw new Error("Simulated crash");
            }
        }, null, {
            // isGreat: false
        });
        
    } catch(e) {
        console.error(e);
        process.exit(1);
    }

})();