# RabbitMQ Web Dispatch

rabbitmq-web-dispatch is a thin veneer around Cowboy that provides the
ability for multiple applications to co-exist on Cowboy
listeners. Applications can register static document roots or dynamic
handlers to be executed, dispatched by URL path prefix.

See

 * [Management plugin guide](http://www.rabbitmq.com/management.html)
 * [Web STOMP guide](http://www.rabbitmq.com/web-stomp.html)
 * [Web MQTT guide](http://www.rabbitmq.com/web-mqtt.html)

for information on configuring plugins that expose an HTTP or WebSocket interface.
