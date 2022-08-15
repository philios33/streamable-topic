import { MongoClient } from "mongodb";

// Creates and startsup a single MongoClient instance which is reusable

export class MongoClientController {
    private connectionId: string
    private mongoUrl: string

    private client: null | MongoClient
    private isStarting: boolean
    private isStarted: boolean

    constructor(mongoUrl: string, connectionId: string) {
        this.mongoUrl = mongoUrl;
        this.connectionId = connectionId;

        this.client = null;
        this.isStarting = false;
        this.isStarted = false;
    }

    async start() {
        if (this.isStarting) {
            return false; // Already started up by something else
        }
        this.isStarting = true;

        const client = new MongoClient(this.mongoUrl, {
            loggerLevel: "info",
            serverSelectionTimeoutMS: 0, // This will retry connection forever
            connectTimeoutMS: 10 * 1000, // This seems to be used deeper down the stack
            socketTimeoutMS: 120 * 1000, // Previously 15 seconds, but this caused unnecessary reconnections
            keepAlive: true,
            keepAliveInitialDelay: 20 * 1000,
            heartbeatFrequencyMS: 20 * 1000,
            maxPoolSize: 1,
            /*
            directConnection: true, // This is important, it forces a single node style connection
            */
        });
        this.client = client;

        await new Promise((resolve, reject) => {
            const timeoutSecs = 60;
            let isReady = false;

            client.addListener("error", (e) => {
                console.error("MONGO CLIENT " + this.connectionId + ": ERROR:", e);
            });
            client.addListener("connectionCreated", (e) => {
                // console.log("MONGO CLIENT " + this.connectionId + ": Connection created:", e);
            });
            client.addListener("connectionReady", (e) => {
                // console.log("MONGO CLIENT " + this.connectionId + ": Connection is ready");
            });
            client.addListener("connectionClosed", (e) => {
                console.error("MONGO CLIENT " + this.connectionId + ": Connection closed:", e);
            });
            client.addListener("serverClosed", (e) => {
                console.error("MONGO CLIENT " + this.connectionId + ": Server closed:", e);
            });
            client.addListener("serverOpening", (e) => {
                // console.log("MONGO CLIENT " + this.connectionId + ": Server opening:", e);
            });

            client.connect((err) => {
                if (err) {
                    console.error('Error connecting to mongo: ' + err);
                    console.warn('Could not connect to mongo at: ' + this.mongoUrl);
                    reject(err);
                } else {
                    // console.log('Connected successfully to mongo');
                    resolve(null);
                }
            });
            setTimeout(() => {
                if (!isReady) {
                    reject(new Error("Timeout for mongo connection to become ready"));
                }
            }, timeoutSecs * 1000);
        });
        this.isStarted = true;
    }

    getClient() : MongoClient {
        if (this.client === null) {
            throw new Error("You must startup this controller");
        } else {
            if (this.isStarted) {
                return this.client;
            } else {
                throw new Error("Mongo client still starting up, please await the startup function properly");
            }
        }
    }
    
}