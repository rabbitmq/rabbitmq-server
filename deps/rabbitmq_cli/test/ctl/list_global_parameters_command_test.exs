## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2020 VMware, Inc. or its affiliates.  All rights reserved.


defmodule ListGlobalParametersCommandTest do
  use ExUnit.Case, async: false
  import TestHelper

  @command RabbitMQ.CLI.Ctl.Commands.ListGlobalParametersCommand

  @key :mqtt_default_vhosts
  @value "{\"O=client,CN=dummy\":\"somevhost\"}"

  setup_all do
    RabbitMQ.CLI.Core.Distribution.start()
    :ok
  end

  setup context do
    on_exit(fn ->
      clear_global_parameter context[:key]
    end)

    {
      :ok,
      opts: %{
        node: get_rabbit_hostname(),
        timeout: (context[:timeout] || :infinity),
      }
    }
  end

  test "validate: wrong number of arguments leads to an arg count error" do
    assert @command.validate(["this", "is", "too", "many"], %{}) == {:validation_failure, :too_many_args}
  end

  @tag key: @key, value: @value
  test "run: a well-formed command returns list of global parameters", context do
    set_global_parameter(context[:key], @value)
    @command.run([], context[:opts])
    |> assert_parameter_list(context)
  end

  @tag key: @key, value: @value
  test "run: zero timeout return badrpc", context do
    set_global_parameter(context[:key], @value)
    assert @command.run([], Map.put(context[:opts], :timeout, 0)) == {:badrpc, :timeout}
  end

  test "run: multiple parameters returned in list", context do
    initial = for param <- @command.run([], context[:opts]), do: Map.new(param)
    parameters = [
      %{name: :global_param_1, value: "{\"key1\":\"value1\"}"},
      %{name: :global_param_2, value: "{\"key2\":\"value2\"}"}
    ]


    Enum.each(parameters, fn(%{name: name, value: value}) ->
      set_global_parameter(name, value)
      on_exit(fn ->
        clear_global_parameter(name)
      end)
    end)

    parameters = initial ++ parameters
    params     = for param <- @command.run([], context[:opts]), do: Map.new(param)

    assert MapSet.new(params) == MapSet.new(parameters)
  end

  @tag key: @key, value: @value
  test "banner", context do
    assert @command.banner([], context[:opts])
      =~ ~r/Listing global runtime parameters \.\.\./
  end

  # Checks each element of the first parameter against the expected context values
  defp assert_parameter_list(params, context) do
    [param | _] = params
    assert MapSet.new(param) == MapSet.new([name: context[:key],
                                            value: context[:value]])
  end
end
