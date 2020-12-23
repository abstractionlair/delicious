A Distributed Lisp-like Interpreter with Caching
================================================
I wanted a good project to try out AWS.
So far it only has a local-Python proof of concept, using processes rather than threads since that seems closer to what it would be like on EC2 instances.
And it uses Redis.
I haven't yet decided whether to just launch some Linux machines and setup all the software myself or use things like SQS, Elasticache, ...

