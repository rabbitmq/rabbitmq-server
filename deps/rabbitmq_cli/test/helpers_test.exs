defmodule HelpersTest do
  use ExUnit.Case, async: true
  import TestHelper

  setup_all do
    :net_kernel.start([:elixirmqctl, :shortnames])
    on_exit([], fn -> :net_kernel.stop() end)
    :ok
  end

  setup context do
    :net_kernel.connect_node(context[:target])
    on_exit(context, fn -> :erlang.disconnect_node(context[:target]) end)
    :ok
  end

  test "RabbitMQ hostname is properly formed" do
    assert Helpers.get_rabbit_hostname() |> Atom.to_string() =~ ~r/rabbit@\w+/
  end

  test "RabbitMQ default hostname connects" do
    assert Helpers.connect_to_rabbitmq() == true
  end

  @tag target: get_rabbit_hostname()
  test "RabbitMQ specified hostname atom connects", context do
    assert Helpers.connect_to_rabbitmq(context[:target]) == true
  end

  @tag target: :jake@thedog
  test "Invalid specified hostname atom doesn't connect", context do
    assert Helpers.connect_to_rabbitmq(context[:target]) == false
  end
end
