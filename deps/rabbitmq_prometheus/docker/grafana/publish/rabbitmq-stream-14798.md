# RabbitMQ-Stream

The stream protocol has been introduced in RabbitMQ 3.9.0, and is meant to be used in conjuction with Streams.

Streams are a new persistent and replicated data structure which models an append-only log with non-destructive consumer semantics.

Learn more about [RabbitMQ Streams](https://www.rabbitmq.com/streams.html).

These blog posts expand on the documentation:
- [Streams Overview](https://blog.rabbitmq.com/posts/2021/07/rabbitmq-streams-overview/) (includes slides)
- [First Application with Streams](https://blog.rabbitmq.com/posts/2021/07/rabbitmq-streams-first-application/) (includes video)
- [Connecting to Streams](https://blog.rabbitmq.com/posts/2021/07/connecting-to-streams/) (includes diagrams)

## Metrics displayed

- Stream publishers
- Stream messages received / s
- Stream messages confirmed to publishers / s
- Stream consumers
- Stream messages delivered / s
- Errors since boot

## Filter by

- Namespace
- RabbitMQ Cluster


## Requires

- `rabbitmq-stream` plugin to be enabled
- `rabbitmq-prometheus` plugin to be enabled
