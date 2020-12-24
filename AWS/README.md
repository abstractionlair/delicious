SQS
===

https://softwaremill.com/amazon-sqs-performance-latency/
2017
NOTE: I don't think these were FIFO queues
Rounding to be conservative

* One sender -> SQS -> One receiver should work up to 5000 messages per second.
* Almost linear scaling adding senders and receivers.
* 100ms to send a message
* Up to 1300ms to receive a message after sending.

This seems like a bad fit for the *back and forth* message passing I have in mind.


What to cache / distribute
==========================
We could divide functions into categories like
1. Short: just calculate in process.
2. Medium: worth distributing / caching on the same machine
3. Long: worth distributing / caching across machines

Within the in-process domain, it would also be nice to do db-read batching.