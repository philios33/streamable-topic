import { MongoClient } from "mongodb";

// Creates and startsup a single MongoClient instance which is reusable

export class MongoClientController {
    private connectionId: string
    private mongoUrl: string

    private client: null | MongoClient
    private isStarting: boolean
    private isStarted: boolean
    private isStopped: boolean

    constructor(mongoUrl: string, connectionId: string) {
        this.mongoUrl = mongoUrl;
        this.connectionId = connectionId;

        this.client = null;
        this.isStarting = false;
        this.isStarted = false;
        this.isStopped = false;
    }

    async start() {
        if (this.isStopped) {
            return false; // Already stopped
        }
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
                // Not an error, just that the server connection closed, probably by the pool controller because it is not needed anymore
                // console.error("MONGO CLIENT " + this.connectionId + ": Connection closed:", e);
            });
            client.addListener("serverClosed", (e) => {
                // Not an error, just that the server connection closed, probably by the pool controller because it is not needed anymore
                // console.error("MONGO CLIENT " + this.connectionId + ": Server closed:", e);
            });
            client.addListener("serverOpening", (e) => {
                console.log("MONGO CLIENT " + this.connectionId + ": Server opening...:", e);
            });
            

            const theTimeout = setTimeout(() => {
                reject(new Error("Timeout for mongo connection to become ready"));
            }, timeoutSecs * 1000);

            // Check for stop during startup
            const theInterval = setInterval(() => {
                if (this.isStopped) {
                    finish(false, new Error("MCC is stopped"));
                }
            }, 1 * 1000);

            const finish = (resolved: boolean, response: any) => {
                clearTimeout(theTimeout);
                clearInterval(theInterval);
                if (resolved) {
                    resolve(response);
                } else {
                    reject(response);
                }
            }

            client.connect((err) => {
                if (err) {
                    console.error('Error connecting to mongo: ' + err);
                    console.warn('Could not connect to mongo at: ' + this.mongoUrl);
                    finish(false, err);
                } else {
                    // console.log('Connected successfully to mongo');
                    finish(true, null);
                }
            });
        });
        this.isStarted = true;
    }

    async stop() {
        if (this.isStopped) {
            return false; // Already stopped
        }
        this.isStopped = true;
        this.client?.close();
        this.client = null;
    }

    getClient() : MongoClient {
        if (this.isStopped) {
            throw new Error("This MCC is stopped");
        }
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