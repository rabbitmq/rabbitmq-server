## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2023 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries.  All rights reserved.

defmodule DisablePluginsCommandTest do
  use ExUnit.Case, async: false
  import TestHelper

  alias RabbitMQ.CLI.Core.ExitCodes

  @command RabbitMQ.CLI.Plugins.Commands.DisableCommand

  setup_all do
    RabbitMQ.CLI.Core.Distribution.start()
    node = get_rabbit_hostname()

    {:ok, plugins_file} =
      :rabbit_misc.rpc_call(node, :application, :get_env, [:rabbit, :enabled_plugins_file])

    {:ok, plugins_dir} =
      :rabbit_misc.rpc_call(node, :application, :get_env, [:rabbit, :plugins_dir])

    rabbitmq_home = :rabbit_misc.rpc_call(node, :code, :lib_dir, [:rabbit])

    IO.puts(
      "plugins disable tests default env: enabled plugins = #{plugins_file}, plugins dir = #{plugins_dir}, RabbitMQ home directory = #{rabbitmq_home}"
    )

    {:ok, [enabled_plugins]} = :file.consult(plugins_file)

    IO.puts(
      "plugins disable tests will assume tnat #{Enum.join(enabled_plugins, ",")} is the list of enabled plugins to revert to"
    )

    opts = %{
      enabled_plugins_file: plugins_file,
      plugins_dir: plugins_dir,
      rabbitmq_home: rabbitmq_home,
      online: false,
      offline: false,
      all: false
    }

    on_exit(fn ->
      set_enabled_plugins(enabled_plugins, :online, get_rabbit_hostname(), opts)
    end)

    {:ok, opts: opts}
  end

  setup context do
    set_enabled_plugins(
      [:rabbitmq_stomp, :rabbitmq_federation],
      :online,
      get_rabbit_hostname(),
      context[:opts]
    )

    {
      :ok,
      opts:
        Map.merge(context[:opts], %{
          node: get_rabbit_hostname(),
          timeout: 1000
        })
    }
  end

  # Helper functions for order-insensitive assertions
  defp normalize_result_map(map) when is_map(map) do
    map
    |> Map.update(:stopped, [], &Enum.sort/1)
    |> Map.update(:disabled, [], &Enum.sort/1)
    |> Map.update(:set, [], &Enum.sort/1)
  end

  defp normalize_stream_result([list, map]) when is_list(list) and is_map(map) do
    [Enum.sort(list), normalize_result_map(map)]
  end

  defp normalize_stream_result(other), do: other

  defp assert_lists_equal(expected, actual) do
    assert Enum.sort(expected) == Enum.sort(actual)
  end

  test "validate: specifying both --online and --offline is reported as invalid", context do
    assert match?(
             {:validation_failure, {:bad_argument, _}},
             @command.validate(["a"], Map.merge(context[:opts], %{online: true, offline: true}))
           )
  end

  test "validate: not specifying plugins to enable is reported as invalid", context do
    assert match?(
             {:validation_failure, :not_enough_args},
             @command.validate([], Map.merge(context[:opts], %{online: true, offline: false}))
           )
  end

  test "validate_execution_environment: specifying a non-existent enabled_plugins_file is fine",
       context do
    assert @command.validate_execution_environment(
             ["a"],
             Map.merge(context[:opts], %{enabled_plugins_file: "none"})
           ) == :ok
  end

  test "validate_execution_environment: specifying a non-existent plugins_dir is reported as an error",
       context do
    assert @command.validate_execution_environment(
             ["a"],
             Map.merge(context[:opts], %{plugins_dir: "none"})
           ) ==
             {:validation_failure, :plugins_dir_does_not_exist}
  end

  test "node is inaccessible, writes out enabled plugins file and returns implicitly enabled plugin list",
       context do
    assert {:stream, test_stream} =
             @command.run(["rabbitmq_stomp"], Map.merge(context[:opts], %{node: :nonode}))

    result = Enum.to_list(test_stream)
    expected = [
      [:rabbitmq_exchange_federation, :rabbitmq_federation, :rabbitmq_federation_common, :rabbitmq_queue_federation],
      %{mode: :offline, disabled: [:rabbitmq_stomp], set: [:rabbitmq_exchange_federation, :rabbitmq_federation, :rabbitmq_federation_common, :rabbitmq_queue_federation]}
    ]
    assert normalize_stream_result(expected) == normalize_stream_result(result)

    assert {:ok, [[:rabbitmq_federation]]} == :file.consult(context[:opts][:enabled_plugins_file])

    result = :rabbit_misc.rpc_call(context[:opts][:node], :rabbit_plugins, :active, [])
    expected = [:amqp_client, :rabbitmq_exchange_federation, :rabbitmq_federation, :rabbitmq_federation_common, :rabbitmq_queue_federation, :rabbitmq_stomp]
    assert_lists_equal(expected, result)
  end

  test "in offline mode, writes out enabled plugins and reports implicitly enabled plugin list",
       context do
    assert {:stream, test_stream} =
             @command.run(
               ["rabbitmq_stomp"],
               Map.merge(context[:opts], %{offline: true, online: false})
             )

    result = Enum.to_list(test_stream)
    expected = [
      [:rabbitmq_exchange_federation, :rabbitmq_federation, :rabbitmq_federation_common, :rabbitmq_queue_federation],
      %{mode: :offline, disabled: [:rabbitmq_stomp], set: [:rabbitmq_exchange_federation, :rabbitmq_federation, :rabbitmq_federation_common, :rabbitmq_queue_federation]}
    ]
    assert normalize_stream_result(expected) == normalize_stream_result(result)

    assert {:ok, [[:rabbitmq_federation]]} == :file.consult(context[:opts][:enabled_plugins_file])

    active_plugins = :rabbit_misc.rpc_call(context[:opts][:node], :rabbit_plugins, :active, [])
    expected_active = [:amqp_client, :rabbitmq_exchange_federation, :rabbitmq_federation, :rabbitmq_federation_common, :rabbitmq_queue_federation, :rabbitmq_stomp]
    assert_lists_equal(expected_active, active_plugins)
  end

  test "in offline mode, removes implicitly enabled plugins when the last explicitly enabled one is removed",
       context do
    assert {:stream, test_stream0} =
             @command.run(
               ["rabbitmq_federation"],
               Map.merge(context[:opts], %{offline: true, online: false})
             )

    result = Enum.to_list(test_stream0)
    expected = [
      [:rabbitmq_stomp],
      %{mode: :offline, disabled: [:rabbitmq_exchange_federation, :rabbitmq_federation, :rabbitmq_federation_common, :rabbitmq_queue_federation], set: [:rabbitmq_stomp]}
    ]
    assert normalize_stream_result(expected) == normalize_stream_result(result)

    assert {:ok, [[:rabbitmq_stomp]]} == :file.consult(context[:opts][:enabled_plugins_file])

    assert {:stream, test_stream1} =
             @command.run(
               ["rabbitmq_stomp"],
               Map.merge(context[:opts], %{offline: true, online: false})
             )

    result = Enum.to_list(test_stream1)
    expected = [[], %{mode: :offline, disabled: [:rabbitmq_stomp], set: []}]
    assert normalize_stream_result(expected) == normalize_stream_result(result)

    assert {:ok, [[]]} = :file.consult(context[:opts][:enabled_plugins_file])
  end

  test "updates plugin list and stops disabled plugins", context do
    assert {:stream, test_stream0} = @command.run(["rabbitmq_stomp"], context[:opts])

    result = Enum.to_list(test_stream0)
    expected = [
      [:rabbitmq_exchange_federation, :rabbitmq_federation, :rabbitmq_federation_common, :rabbitmq_queue_federation],
      %{
        mode: :online,
        started: [],
        stopped: [:rabbitmq_stomp],
        disabled: [:rabbitmq_stomp],
        set: [:rabbitmq_exchange_federation, :rabbitmq_federation, :rabbitmq_federation_common, :rabbitmq_queue_federation]
      }
    ]
    assert normalize_stream_result(expected) == normalize_stream_result(result)

    assert {:ok, [[:rabbitmq_federation]]} == :file.consult(context[:opts][:enabled_plugins_file])

    result = :rabbit_misc.rpc_call(context[:opts][:node], :rabbit_plugins, :active, [])
    expected = [:amqp_client, :rabbitmq_exchange_federation, :rabbitmq_federation, :rabbitmq_federation_common, :rabbitmq_queue_federation]
    assert_lists_equal(expected, result)

    assert {:stream, test_stream1} = @command.run(["rabbitmq_federation"], context[:opts])

    result = Enum.to_list(test_stream1)
    expected = [
      [],
      %{
        mode: :online,
        started: [],
        stopped: [:rabbitmq_exchange_federation, :rabbitmq_federation, :rabbitmq_federation_common, :rabbitmq_queue_federation],
        disabled: [:rabbitmq_federation_common, :rabbitmq_queue_federation, :rabbitmq_exchange_federation, :rabbitmq_federation],
        set: []
      }
    ]
    assert normalize_stream_result(expected) == normalize_stream_result(result)

    assert {:ok, [[]]} == :file.consult(context[:opts][:enabled_plugins_file])

    result = :rabbit_misc.rpc_call(context[:opts][:node], :rabbit_plugins, :active, [])
    assert Enum.empty?(result)
  end

  test "can disable multiple plugins at once", context do
    assert {:stream, test_stream} =
             @command.run(["rabbitmq_stomp", "rabbitmq_federation"], context[:opts])

    result = Enum.to_list(test_stream)
    expected_list = [:rabbitmq_exchange_federation, :rabbitmq_federation, :rabbitmq_federation_common, :rabbitmq_queue_federation, :rabbitmq_stomp]
    expected = [
      [],
      %{
        mode: :online,
        started: [],
        stopped: expected_list,
        disabled: expected_list,
        set: []
      }
    ]
    assert normalize_stream_result(expected) == normalize_stream_result(result)

    assert {:ok, [[]]} == :file.consult(context[:opts][:enabled_plugins_file])

    active_plugins = :rabbit_misc.rpc_call(context[:opts][:node], :rabbit_plugins, :active, [])
    assert Enum.empty?(active_plugins)
  end

  test "disabling a dependency disables all plugins that depend on it", context do
    assert {:stream, test_stream} = @command.run(["amqp_client"], context[:opts])
    result = Enum.to_list(test_stream)
    expected_list = [:rabbitmq_exchange_federation, :rabbitmq_federation, :rabbitmq_federation_common, :rabbitmq_queue_federation, :rabbitmq_stomp]
    expected = [
      [],
      %{
        mode: :online,
        started: [],
        stopped: expected_list,
        disabled: expected_list,
        set: []
      }
    ]
    assert normalize_stream_result(expected) == normalize_stream_result(result)

    assert {:ok, [[]]} == :file.consult(context[:opts][:enabled_plugins_file])

    result = :rabbit_misc.rpc_call(context[:opts][:node], :rabbit_plugins, :active, [])
    assert Enum.empty?(result)
  end

  test "formats enabled plugins mismatch errors", context do
    err = {:enabled_plugins_mismatch, ~c"/tmp/a/cli/path", ~c"/tmp/a/server/path"}

    assert {:error, ExitCodes.exit_dataerr(),
            "Could not update enabled plugins file at /tmp/a/cli/path: target node #{context[:opts][:node]} uses a different path (/tmp/a/server/path)"} ==
             @command.output({:error, err}, context[:opts])
  end

  test "formats enabled plugins write errors", context do
    err1 = {:cannot_write_enabled_plugins_file, "/tmp/a/path", :eacces}

    assert {:error, ExitCodes.exit_dataerr(),
            "Could not update enabled plugins file at /tmp/a/path: the file does not exist or permission was denied (EACCES)"} ==
             @command.output({:error, err1}, context[:opts])

    err2 = {:cannot_write_enabled_plugins_file, "/tmp/a/path", :enoent}

    assert {:error, ExitCodes.exit_dataerr(),
            "Could not update enabled plugins file at /tmp/a/path: the file does not exist (ENOENT)"} ==
             @command.output({:error, err2}, context[:opts])
  end
end
