## The contents of this file are subject to the Mozilla Public License
## Version 1.1 (the "License"); you may not use this file except in
## compliance with the License. You may obtain a copy of the License
## at https://www.mozilla.org/MPL/
##
## Software distributed under the License is distributed on an "AS IS"
## basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
## the License for the specific language governing rights and
## limitations under the License.
##
## The Original Code is RabbitMQ.
##
## The Initial Developer of the Original Code is GoPivotal, Inc.
## Copyright (c) 2007-2019 Pivotal Software, Inc.  All rights reserved.


defmodule ClearGlobalParameterCommandTest do
  use ExUnit.Case, async: false
  import TestHelper

  @command RabbitMQ.CLI.Ctl.Commands.ClearGlobalParameterCommand
  @key :mqtt_default_vhosts
  @value "{\"O=client,CN=dummy\":\"somevhost\"}"

  setup_all do
    RabbitMQ.CLI.Core.Distribution.start()




    :ok
  end

  setup context do
    on_exit(context, fn ->
      clear_global_parameter context[:key]
    end)

    {
      :ok,
      opts: %{
        node: get_rabbit_hostname()
      }
    }
  end

  test "validate: expects a single argument" do
    assert @command.validate(["one"], %{}) == :ok
    assert @command.validate([], %{}) == {:validation_failure, :not_enough_args}
    assert @command.validate(["this is", "too many"], %{}) == {:validation_failure, :too_many_args}
  end

  @tag key: @key
  test "run: when global parameter does not exist, returns an error", context do
    assert @command.run(
      [context[:key]],
      context[:opts]
    ) == {:error_string, 'Parameter does not exist'}
  end

  test "run: throws a badrpc when instructed to contact an unreachable RabbitMQ node" do
    opts = %{node: :jake@thedog, timeout: 200}
    assert match?({:badrpc, _}, @command.run([@key], opts))
  end

  @tag key: @key
  test "run: clears the parameter", context do
    set_global_parameter(context[:key], @value)

    assert @command.run(
      [context[:key]],
      context[:opts]
    ) == :ok

    assert_parameter_empty(context)
  end

  @tag key: @key, value: @value
  test "banner", context do
    set_global_parameter(context[:key], @value)

    s = @command.banner(
      [context[:key]],
      context[:opts]
    )

    assert s =~ ~r/Clearing global runtime parameter/
    assert s =~ ~r/"#{context[:key]}"/
  end

  defp assert_parameter_empty(context) do
    parameter = list_global_parameters()
                |> Enum.filter(fn(param) ->
                    param[:key] == context[:key]
                    end)
    assert parameter === []
  end
end
