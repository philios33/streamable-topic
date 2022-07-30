# Mongo Topic Implementation

Event streaming concepts implemented on top of a persistent mongo storage engine.

We use a nextId auto incrementing sequence concept (stored in the "nextids" collection) for ordering messages.  

Since a write must get the next id first before writing the payload, writing a new message takes two operations to store properly.

ObjectID was not appropriate here.

You can push new messages to the end of a stream, and consume messages starting from some position.
A consumer keeps track of the last message consumed so it can poll for the next set.

The concept is relatively simple but a powerful building block for event based systems when implemented properly.

