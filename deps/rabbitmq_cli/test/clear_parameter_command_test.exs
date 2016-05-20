## The contents of this file are subject to the Mozilla Public License
## Version 1.1 (the "License"); you may not use this file except in
## compliance with the License. You may obtain a copy of the License
## at http://www.mozilla.org/MPL/
##
## Software distributed under the License is distributed on an "AS IS"
## basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
## the License for the specific language governing rights and
## limitations under the License.
##
## The Original Code is RabbitMQ.
##
## The Initial Developer of the Original Code is GoPivotal, Inc.
## Copyright (c) 2007-2016 Pivotal Software, Inc.  All rights reserved.


defmodule ClearParameterCommandTest do
  use ExUnit.Case, async: false
  import ExUnit.CaptureIO
  import TestHelper

  @vhost "test1"
  @user "guest"
  @root   "/"
  @component_name "federation-upstream"
  @key "reconnect-delay"
  @value "{\"uri\":\"amqp://\"}"

  setup_all do
    :net_kernel.start([:rabbitmqctl, :shortnames])
    :net_kernel.connect_node(get_rabbit_hostname)

    add_vhost @vhost

    on_exit([], fn ->
      delete_vhost @vhost
      :erlang.disconnect_node(get_rabbit_hostname)
      :net_kernel.stop()
    end)

    :ok
  end

  setup context do

    on_exit(context, fn ->
      clear_parameter context[:vhost], context[:component_name], context[:key]
    end)

    {
      :ok,
      opts: %{
        node: get_rabbit_hostname
      }
    }
  end

  test "wrong number of arguments leads to an arg count error" do
    assert ClearParameterCommand.run([], %{}) == {:not_enough_args, []}
    assert ClearParameterCommand.run(["insufficient"], %{}) == {:not_enough_args, ["insufficient"]}
    assert ClearParameterCommand.run(["this", "is", "many"], %{}) == {:too_many_args, ["this", "is", "many"]}
  end

  @tag component_name: @component_name, key: @key, vhost: @vhost
  test "returns error, if parameter does not exist", context do
    vhost_opts = Map.merge(context[:opts], %{param: context[:vhost]})

    capture_io(fn ->
      assert ClearParameterCommand.run(
        [context[:component_name], context[:key]],
        vhost_opts
      ) == {:error_string, 'Parameter does not exist'}
    end)
  end

  test "An invalid rabbitmq node throws a badrpc" do
    target = :jake@thedog
    :net_kernel.connect_node(target)
    opts = %{node: target}

    capture_io(fn ->
      assert ClearParameterCommand.run([@component_name, @key], opts) == {:badrpc, :nodedown}
    end)
  end


  @tag component_name: @component_name, key: @key, vhost: @vhost
  test "returns ok and clears parameter, if it exists", context do
    vhost_opts = Map.merge(context[:opts], %{param: context[:vhost]})

    set_parameter(context[:vhost], context[:component_name], context[:key], @value)

    capture_io(fn ->
      assert ClearParameterCommand.run(
        [context[:component_name], context[:key]],
        vhost_opts
      ) == :ok
    end)

    assert_parameter_empty(context)
  end

  @tag component_name: @component_name, key: @key
  test "run on default vhost by default", context do
    set_parameter("/", context[:component_name], context[:key], @value)

    capture_io(fn ->
      assert ClearParameterCommand.run(
        [context[:component_name], context[:key]],
        context[:opts]
      ) == :ok
    end)

    context
    |> Map.merge(%{vhost: "/"})
    |> assert_parameter_empty
  end

  @tag component_name: "bad-component-name", key: @key, value: @value, vhost: @root
  test "an invalid component_name returns a 'parameter does not exist' error", context do
    capture_io(fn ->
      assert ClearParameterCommand.run(
        [context[:component_name], context[:key]],
        context[:opts]
      ) == {:error_string, 'Parameter does not exist'}
    end)

    assert list_parameters(context[:vhost]) == []
  end

  @tag component_name: @component_name, key: @key, value: @value, vhost: "bad-vhost"
  test "an invalid vhost returns a 'parameter does not exist' error", context do
    vhost_opts = Map.merge(context[:opts], %{param: context[:vhost]})

    capture_io(fn ->
      assert ClearParameterCommand.run(
        [context[:component_name], context[:key]],
        vhost_opts
      ) == {:error_string, 'Parameter does not exist'}
    end)
  end

  @tag component_name: @component_name, key: @key, value: @value, vhost: @vhost
  test "the info message prints by default", context do
    vhost_opts = Map.merge(context[:opts], %{param: context[:vhost]})
    set_parameter(context[:vhost], context[:component_name], context[:key], @value)

    assert capture_io(fn ->
      ClearParameterCommand.run(
        [context[:component_name], context[:key]],
        vhost_opts
      )
    end) =~ ~r/Clearing runtime parameter "#{context[:key]}" for component "#{context[:component_name]}" on vhost "#{context[:vhost]}" \.\.\./
  end

  @tag component_name: @component_name, key: @key, vhost: @vhost
  test "the --quiet option suppresses the info message", context do
    vhost_opts = Map.merge(context[:opts], %{param: context[:vhost], quiet: true})
    set_parameter(context[:vhost], context[:component_name], context[:key], @value)

    refute capture_io(fn ->
      ClearParameterCommand.run(
        [context[:component_name], context[:key]],
        vhost_opts
      )
    end) =~ ~r/Clearing runtime parameter "#{context[:key]} for component "#{context[:component_name]}" on vhost "#{context[:vhost]}" \.\.\./
  end

  defp assert_parameter_empty(context) do
    parameter = context[:vhost]
                |> list_parameters
                |> Enum.filter(fn(param) ->
                    param[:component_name] == context[:component_name] and
                    param[:key] == context[:key]
                    end)
    assert parameter === []
  end
end
