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


defmodule ListParameterCommandTest do
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
    assert ListParameterCommand.run(["this", "is", "too", "many"], %{}) == {:too_many_args, ["this", "is", "too", "many"]}
  end

  @tag component_name: @component_name, key: @key, value: @value, vhost: @vhost
  test "a well-formed, host-specific command returns list of parameters", context do
    vhost_opts = Map.merge(context[:opts], %{vhost: context[:vhost]})

    capture_io(fn ->
      assert ListParameterCommand.run(
        [],
        vhost_opts
      ) == params
      assert_parameter_list(context, params)
    end)
  end

  test "An invalid rabbitmq node throws a badrpc" do
    target = :jake@thedog
    :net_kernel.connect_node(target)
    opts = %{node: target}

    capture_io(fn ->
      assert ListParameterCommand.run([@component_name, @key, @value], opts) == {:badrpc, :nodedown}
    end)
  end

  @tag component_name: @component_name, key: @key, value: @value, vhost: @root
  test "a well-formed command with no vhost runs against the default", context do

    set_parameter("/", context[:component_name], context[:key], @value)

    capture_io(fn ->
      assert ListParameterCommand.run(
        [],
        context[:opts]
      ) == params

      assert_parameter_list(context, params)
    end)
  end

  @tag component_name: @component_name, key: @key, value: @value, vhost: "bad-vhost"
  test "an invalid vhost returns a no-such-vhost error", context do
    vhost_opts = Map.merge(context[:opts], %{vhost: context[:vhost]})

    capture_io(fn ->
      assert ListParameterCommand.run(
        [],
        vhost_opts
      ) == {:error, {:no_such_vhost, context[:vhost]}}
    end)
  end

  @tag vhost: @root
  test "multiple parameters returned in list", context do
    parameters = [
      %{component: "federation-upstream", name: "my-upstream", value: "{\"uri\":\"amqp://\"}"},
      %{component: "exchange-delete-in-progress", name: "my-key", value: "{}"}
    ]

    parameters
    |> List.map(
        fn(%{component: component, name: name, value: value}) ->
          set_parameter(context[:vhost], component, name, value)
        end)
    
    capture_io(fn ->
      assert ListParameterCommand.run(
        [],
        context[:opts]
      ) == params

      assert MapSet.new(params) == MapSet.new(parameters)
    end)
  end

  @tag component_name: @component_name, key: @key, value: @value, vhost: @vhost
  test "the info message prints by default", context do
    vhost_opts = Map.merge(context[:opts], %{vhost: context[:vhost]})

    assert capture_io(fn ->
      ListParameterCommand.run(
        [],
        vhost_opts
      )
    end) =~ ~r/Listing runtime parameters for vhost \"#{context[:vhost]}\" \.\.\./
  end

  @tag component_name: @component_name, key: @key, value: @value, vhost: @vhost
  test "the --quiet option suppresses the info message", context do
    vhost_opts = Map.merge(context[:opts], %{vhost: context[:vhost], quiet: true})

    refute capture_io(fn ->
      ListParameterCommand.run(
        [context[:component_name], context[:key], context[:value]],
        vhost_opts
      )
    end) =~ ~r/Listing runtime parameters for vhost \"#{context[:vhost]}\" \.\.\./
  end

  # Checks each element of the first parameter against the expected context values
  defp assert_parameter_list(context, params) do
    assert MapSet.new(params) == MapSet.new([%{component: context[:component_name],
                                               name: context[:key],
                                               value: context[:value]}])
  end
end
