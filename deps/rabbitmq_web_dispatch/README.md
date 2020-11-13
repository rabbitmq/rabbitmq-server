rabbitmq-web-dispatch
---------------------

rabbitmq-web-dispatch is a thin veneer around Cowboy that provides the
ability for multiple applications to co-exist on Cowboy
listeners. Applications can register static docroots or dynamic
handlers to be executed, dispatched by URL path prefix.

Related doc guides:

 * [Management plugin guide](https://www.rabbitmq.com/management.html)
 * [Web STOMP guide](https://www.rabbitmq.com/web-stomp.html)
 * [Web MQTT guide](https://www.rabbitmq.com/web-mqtt.html)

This takes a dispatch list of the kind usually given to Cowboy, in compiled
form.
