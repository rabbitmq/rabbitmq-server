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


defmodule SetParameterCommandTest do
  use ExUnit.Case, async: false
  import TestHelper
  import ExUnit.CaptureIO

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

  test "wrong number of arguments leads to usage and bad_arg" do
    assert capture_io(fn ->
      assert SetParameterCommand.set_parameter([], %{}) == {:bad_argument, []}
    end) =~ ~r/Usage:\n/

    assert capture_io(fn ->
      assert SetParameterCommand.set_parameter(["insufficient"], %{}) == {:bad_argument, ["insufficient"]}
    end) =~ ~r/Usage:\n/

    assert capture_io(fn ->
      assert SetParameterCommand.set_parameter(["not", "enough"], %{}) == {:bad_argument, ["not", "enough"]}
    end) =~ ~r/Usage:\n/

    assert capture_io(fn ->
      assert SetParameterCommand.set_parameter(["this", "is", "way", "too", "many"], %{}) == {:bad_argument, ["this", "is", "way", "too", "many"]}
    end) =~ ~r/Usage:\n/
  end

  @tag component_name: @component_name, key: @key, value: @value, vhost: @vhost
  test "a well-formed, host-specific command returns okay", context do
    vhost_opts = Map.merge(context[:opts], %{param: context[:vhost]})
    assert SetParameterCommand.set_parameter(
      [context[:component_name], context[:key], context[:value]],
      vhost_opts
    ) == :ok

    assert_parameter_fields(context)
  end

  test "An invalid rabbitmq node throws a badrpc" do
    target = :jake@thedog
    :net_kernel.connect_node(target)
    opts = %{node: target}

    assert SetParameterCommand.set_parameter([@component_name, @key, @value], opts) == {:badrpc, :nodedown}
  end

  @tag component_name: @component_name, key: @key, value: @value, vhost: @root
  test "a well-formed command with no vhost runs against the default", context do
    assert SetParameterCommand.set_parameter(
      [context[:component_name], context[:key], context[:value]],
      context[:opts]
    ) == :ok

    assert_parameter_fields(context)
  end

  @tag component_name: "bad-component-name", key: @key, value: @value, vhost: @root
  test "an invalid component_name returns a validation failed error", context do
    assert SetParameterCommand.set_parameter(
      [context[:component_name], context[:key], context[:value]],
      context[:opts]
    ) == {:error_string, 'Validation failed\n\ncomponent #{context[:component_name]} not found\n'}

    assert list_parameters(context[:vhost]) == []
  end

  @tag component_name: @component_name, key: @key, value: @value, vhost: "bad-vhost"
  test "an invalid vhost returns a no-such-vhost error", context do
    vhost_opts = Map.merge(context[:opts], %{param: context[:vhost]})
    assert SetParameterCommand.set_parameter(
      [context[:component_name], context[:key], context[:value]],
      vhost_opts
    ) == {:error, {:no_such_vhost, context[:vhost]}}
  end

  @tag component_name: @component_name, key: @key, value: "bad-value", vhost: @root
  test "an invalid value returns a JSON decoding error", context do
    assert SetParameterCommand.set_parameter(
      [context[:component_name], context[:key], context[:value]],
      context[:opts]
    ) == {:error_string, 'JSON decoding error'}

    assert list_parameters(context[:vhost]) == []
  end

  @tag component_name: @component_name, key: @key, value: "{}", vhost: @root
  test "an empty JSON object value returns a key \"uri\" not found error", context do
    assert SetParameterCommand.set_parameter(
      [context[:component_name], context[:key], context[:value]],
      context[:opts]
    ) == {:error_string, 'Validation failed\n\nKey "uri" not found in reconnect-delay\n'}

    assert list_parameters(context[:vhost]) == []
  end

  # Checks each element of the first parameter against the expected context values
  defp assert_parameter_fields(context) do
    result_param = context[:vhost] |> list_parameters |> List.first

    assert result_param[:value] == context[:value]
    assert result_param[:vhost] == context[:vhost]
    assert result_param[:component] == context[:component_name]
    assert result_param[:name] == context[:key]
  end
end
