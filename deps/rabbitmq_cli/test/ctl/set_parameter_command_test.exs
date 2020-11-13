## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2020 VMware, Inc. or its affiliates.  All rights reserved.


defmodule SetParameterCommandTest do
  use ExUnit.Case, async: false
  import TestHelper

  @command RabbitMQ.CLI.Ctl.Commands.SetParameterCommand

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

    # featured in a definitions file imported by other tests
    clear_parameter("/", "federation-upstream", "up-1")

    :ok
  end

  setup context do
    on_exit(context, fn ->
      clear_parameter context[:vhost], context[:component_name], context[:key]
    end)
    {
      :ok,
      opts: %{
        node: get_rabbit_hostname(),
        vhost: context[:vhost]
      }
    }
  end

  @tag component_name: @component_name, key: @key, value: @value, vhost: @root
  test "merge_defaults: a well-formed command with no vhost runs against the default" do
    assert match?({_, %{vhost: "/"}}, @command.merge_defaults([], %{}))
    assert match?({_, %{vhost: "non_default"}}, @command.merge_defaults([], %{vhost: "non_default"}))
  end

  test "validate: wrong number of arguments leads to an arg count error" do
    assert @command.validate([], %{}) == {:validation_failure, :not_enough_args}
    assert @command.validate(["insufficient"], %{}) == {:validation_failure, :not_enough_args}
    assert @command.validate(["not", "enough"], %{}) == {:validation_failure, :not_enough_args}
    assert @command.validate(["this", "is", "too", "many"], %{}) == {:validation_failure, :too_many_args}
  end

  @tag component_name: @component_name, key: @key, value: @value, vhost: @vhost
  test "run: a well-formed, host-specific command returns okay", context do
    vhost_opts = Map.merge(context[:opts], %{vhost: context[:vhost]})

    assert @command.run(
      [context[:component_name], context[:key], context[:value]],
      vhost_opts
    ) == :ok

    assert_parameter_fields(context)
  end

  test "run: throws a badrpc when instructed to contact an unreachable RabbitMQ node" do
    opts = %{node: :jake@thedog, vhost: "/", timeout: 200}

    assert match?({:badrpc, _}, @command.run([@component_name, @key, @value], opts))
  end

  @tag component_name: "bad-component-name", key: @key, value: @value, vhost: @root
  test "run: an invalid component_name returns a validation failed error", context do
    assert @command.run(
      [context[:component_name], context[:key], context[:value]],
      context[:opts]
    ) == {:error_string, 'Validation failed\n\ncomponent #{context[:component_name]} not found\n'}

    assert list_parameters(context[:vhost]) == []
  end

  @tag component_name: @component_name, key: @key, value: @value, vhost: "bad-vhost"
  test "run: an invalid vhost returns a no-such-vhost error", context do
    vhost_opts = Map.merge(context[:opts], %{vhost: context[:vhost]})

    assert @command.run(
      [context[:component_name], context[:key], context[:value]],
      vhost_opts
    ) == {:error, {:no_such_vhost, context[:vhost]}}
  end

  @tag component_name: @component_name, key: @key, value: "bad-value", vhost: @root
  test "run: an invalid value returns a JSON decoding error", context do
    assert match?({:error_string, _},
      @command.run([context[:component_name], context[:key], context[:value]],
        context[:opts]))

    assert list_parameters(context[:vhost]) == []
  end

  @tag component_name: @component_name, key: @key, value: "{}", vhost: @root
  test "run: an empty JSON object value returns a key \"uri\" not found error", context do
    assert @command.run(
      [context[:component_name], context[:key], context[:value]],
      context[:opts]
    ) == {:error_string, 'Validation failed\n\nKey "uri" not found in reconnect-delay\n'}

    assert list_parameters(context[:vhost]) == []
  end

  @tag component_name: @component_name, key: @key, value: @value, vhost: @vhost
  test "banner", context do
    vhost_opts = Map.merge(context[:opts], %{vhost: context[:vhost]})

    assert @command.banner([context[:component_name], context[:key], context[:value]], vhost_opts)
      =~ ~r/Setting runtime parameter \"#{context[:key]}\" for component \"#{context[:component_name]}\" to \"#{context[:value]}\" in vhost \"#{context[:vhost]}\" \.\.\./
  end

  # Checks each element of the first parameter against the expected context values
  defp assert_parameter_fields(context) do
    result_param = context[:vhost] |> list_parameters |> List.first

    assert result_param[:value] == context[:value]
    assert result_param[:component] == context[:component_name]
    assert result_param[:name] == context[:key]
  end
end
