# RabbitMQ Stream Plugin

## How to Use

```
git clone git@github.com:rabbitmq/rabbitmq-public-umbrella.git
cd rabbitmq-public-umbrella
make co
make up BRANCH="stream-queue" -j 32
cd deps
git clone git@github.com:rabbitmq/rabbitmq-stream.git rabbitmq_stream
cd rabbitmq_stream
make run-broker
```

Then follow the instructions to [build the client and the performance tool](https://github.com/rabbitmq/rabbitmq-stream-java-client).

## Licensing

Released under the [MPL 2.0](LICENSE-MPL-RabbitMQ).

## Copyright

(c) 2020 VMware, Inc. or its affiliates.