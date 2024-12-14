In RabbitMQ `4.x`, AMQP 1.0 is a [core protocol](https://www.rabbitmq.com/docs/amqp) that is always supported and does not
require a plugin to be enabled.

This no-op plugin exists only such that deployment tools can continue to enable and disable this plugin without erroring:

```
rabbitmq-plugins enable rabbitmq_amqp1_0
rabbitmq-plugins disable rabbitmq_amqp1_0
```

Enabling or disabling this plugin has no effect on AMQP 1.0 support scope or clients.
