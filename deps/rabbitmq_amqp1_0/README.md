This AMQP 1.0 plugin is deprecated and exists only for backward compatibility.

From RabbitMQ `v4.x` onwards, AMQP 1.0 is supported natively by RabbitMQ and all AMQP 1.0 code was moved from this directory to the core [rabbit](../rabbit/) app.

This no-op plugin exists only such that deployment tools can continue to enable and disable this plugin without erroring:
```
rabbitmq-plugins enable rabbitmq_amqp1_0
rabbitmq-plugins disable rabbitmq_amqp1_0
```
Enabling or disabling this plugin has no effect.
RabbitMQ `v4.x` supports AMQP 1.0 by default.
