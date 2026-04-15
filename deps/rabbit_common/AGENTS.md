# RabbitMQ Common

This library (`deps/rabbit_common/`) is only meant to contain code that is shared between RabbitMQ core (`deps/rabbit/`) and the RabbitMQ Erlang AMQP 0.9.1 client (`deps/amqp_client/`).
Only add new code to this library if it's used by both `deps/rabbit/` and `deps/amqp_client/`.
