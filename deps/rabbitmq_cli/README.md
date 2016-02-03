# RabbitMQCtl

An Elixir-based implementation of the [rabbitmqctl](https://www.rabbitmq.com/man/rabbitmqctl.1.man.html) CLI.

This is still very much a work in progress right now. For production use, go with the `rabbitmqctl` distributed with the `rabbitmq-server` repo.

## Installation

If [available in Hex](https://hex.pm/docs/publish), the package can be installed as:

  1. Add elixirmqctl to your list of dependencies in `mix.exs`:

        def deps do
          [{:elixirmqctl, "~> 0.0.1"}]
        end

  2. Ensure elixirmqctl is started before your application:

        def application do
          [applications: [:elixirmqctl]]
        end


## License

RabbitMQCtl is [licensed under the MPL](LICENSE-MPL-RabbitMQ).
