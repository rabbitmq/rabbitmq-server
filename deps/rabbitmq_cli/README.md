# ElixirMQCtl

An Elixir-based implementation of the [rabbitmqctl](https://www.rabbitmq.com/man/rabbitmqctl.1.man.html) CLI.

Very much a work in progress right now.

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

## NEXT UP:
  Get basic program structure to the point where adding a new command is as simple as adding a single file.
