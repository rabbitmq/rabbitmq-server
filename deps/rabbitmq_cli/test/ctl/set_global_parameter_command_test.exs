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


defmodule SetGlobalParameterCommandTest do
  use ExUnit.Case, async: false
  import TestHelper

  @command RabbitMQ.CLI.Ctl.Commands.SetGlobalParameterCommand

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
        node: get_rabbit_hostname(),
      }
    }
  end

  test "validate: expects a key and a value" do
    assert @command.validate([], %{}) == {:validation_failure, :not_enough_args}
    assert @command.validate(["insufficient"], %{}) == {:validation_failure, :not_enough_args}
    assert @command.validate(["this is", "too", "many"], %{}) == {:validation_failure, :too_many_args}
  end

  @tag key: @key, value: @value
  test "run: expects a key and a value", context do
    assert @command.run(
      [context[:key], context[:value]],
      context[:opts]
    ) == :ok

    assert_parameter_fields(context)
  end

  test "run: throws a badrpc when instructed to contact an unreachable RabbitMQ node" do
    opts = %{node: :jake@thedog, timeout: 200}

    assert match?({:badrpc, _}, @command.run([@key, @value], opts))
  end

  @tag key: @key, value: "bad-value"
  test "run: a value that fails to parse as JSON returns a decoding error", context do
    initial = list_global_parameters()
    assert match?({:error_string, _},
      @command.run([context[:key], context[:value]],
        context[:opts]))

    assert list_global_parameters() == initial
  end

  @tag key: @key, value: @value
  test "banner", context do
    assert @command.banner([context[:key], context[:value]], context[:opts])
      =~ ~r/Setting global runtime parameter \"#{context[:key]}\" to \"#{context[:value]}\" \.\.\./
  end

  # Checks each element of the first parameter against the expected context values
  defp assert_parameter_fields(context) do
    result_param = list_global_parameters() |> List.first

    assert result_param[:value] == context[:value]
    assert result_param[:name] == context[:key]
  end

end
