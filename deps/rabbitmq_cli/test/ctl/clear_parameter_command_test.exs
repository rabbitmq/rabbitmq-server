## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2020 VMware, Inc. or its affiliates.  All rights reserved.


defmodule ClearParameterCommandTest do
  use ExUnit.Case, async: false
  import TestHelper

  @command RabbitMQ.CLI.Ctl.Commands.ClearParameterCommand
  @vhost "test1"
  @root   "/"
  @component_name "federation-upstream"
  @key "reconnect-delay"
  @value "{\"uri\":\"amqp://127.0.0.1:5672\"}"

  setup_all do
    RabbitMQ.CLI.Core.Distribution.start()

    add_vhost @vhost

    enable_federation_plugin()

    on_exit([], fn ->
      delete_vhost @vhost
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
        node: get_rabbit_hostname()
      }
    }
  end

  test "merge_defaults: adds default vhost if missing" do
    assert @command.merge_defaults([], %{}) == {[], %{vhost: "/"}}
  end

  test "merge_defaults: defaults can be overridden" do
    assert @command.merge_defaults([], %{}) == {[], %{vhost: "/"}}
    assert @command.merge_defaults([], %{vhost: "non_default"}) == {[], %{vhost: "non_default"}}
  end

  test "validate: argument validation" do
    assert @command.validate(["one", "two"], %{}) == :ok
    assert @command.validate([], %{}) == {:validation_failure, :not_enough_args}
    assert @command.validate(["insufficient"], %{}) == {:validation_failure, :not_enough_args}
    assert @command.validate(["this", "is", "many"], %{}) == {:validation_failure, :too_many_args}
  end

  @tag component_name: @component_name, key: @key, vhost: @vhost
  test "run: returns error, if parameter does not exist", context do
    vhost_opts = Map.merge(context[:opts], %{vhost: context[:vhost]})

    assert @command.run(
      [context[:component_name], context[:key]],
      vhost_opts
    ) == {:error_string, 'Parameter does not exist'}
  end

  test "run: throws a badrpc when instructed to contact an unreachable RabbitMQ node" do
    opts = %{node: :jake@thedog, vhost: "/", timeout: 200}
    assert match?({:badrpc, _}, @command.run([@component_name, @key], opts))
  end


  @tag component_name: @component_name, key: @key, vhost: @vhost
  test "run: returns ok and clears parameter, if it exists", context do
    vhost_opts = Map.merge(context[:opts], %{vhost: context[:vhost]})

    set_parameter(context[:vhost], context[:component_name], context[:key], @value)

    assert @command.run(
      [context[:component_name], context[:key]],
      vhost_opts
    ) == :ok

    assert_parameter_empty(context)
  end

  @tag component_name: "bad-component-name", key: @key, value: @value, vhost: @root
  test "run: an invalid component_name returns a 'parameter does not exist' error", context do
    vhost_opts = Map.merge(context[:opts], %{vhost: context[:vhost]})
    assert @command.run(
      [context[:component_name], context[:key]],
      vhost_opts
    ) == {:error_string, 'Parameter does not exist'}

    assert list_parameters(context[:vhost]) == []
  end

  @tag component_name: @component_name, key: @key, value: @value, vhost: "bad-vhost"
  test "run: an invalid vhost returns a 'parameter does not exist' error", context do
    vhost_opts = Map.merge(context[:opts], %{vhost: context[:vhost]})

    assert @command.run(
      [context[:component_name], context[:key]],
      vhost_opts
    ) == {:error_string, 'Parameter does not exist'}
  end

  @tag component_name: @component_name, key: @key, value: @value, vhost: @vhost
  test "banner", context do
    vhost_opts = Map.merge(context[:opts], %{vhost: context[:vhost]})
    set_parameter(context[:vhost], context[:component_name], context[:key], @value)

    s = @command.banner(
      [context[:component_name], context[:key]],
      vhost_opts
    )

    assert s =~ ~r/Clearing runtime parameter/
    assert s =~ ~r/"#{context[:key]}"/
    assert s =~ ~r/"#{context[:component_name]}"/
    assert s =~ ~r/"#{context[:vhost]}"/
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
