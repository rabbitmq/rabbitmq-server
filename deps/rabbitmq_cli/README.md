# RabbitMQCtl

An Elixir-based implementation of the [rabbitmqctl](https://www.rabbitmq.com/man/rabbitmqctl.1.man.html) CLI.

This is still very much a work in progress right now. For production use, go 
with the `rabbitmqctl` distributed with the `rabbitmq-server` repo.


# Testing

Assuming you have:
 * installed [Elixir](http://elixir-lang.org/install.html)
 * set up an active instance of RabbitMQ

you can simply run `mix test` within the project root directory.

NOTE: You will see the following message several times:

```
warning: variable context is unused
```

This is nothing to be alarmed about; we're currently using `setup context`
function in Mix to start a new distributed node and connect it to the 
RabbitMQ server. It complains because we don't actually use the context 
dictionary, but it's fine otherwise.


## Installation


## License

RabbitMQCtl is [licensed under the MPL](LICENSE-MPL-RabbitMQ).
