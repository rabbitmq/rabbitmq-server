## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2020 VMware, Inc. or its affiliates.  All rights reserved.

defmodule DisablePluginsCommandTest do
  use ExUnit.Case, async: false
  import TestHelper

  alias RabbitMQ.CLI.Core.ExitCodes

  @command RabbitMQ.CLI.Plugins.Commands.DisableCommand

  setup_all do
    RabbitMQ.CLI.Core.Distribution.start()
    node = get_rabbit_hostname()

    {:ok, plugins_file} = :rabbit_misc.rpc_call(node,
                                                :application, :get_env,
                                                [:rabbit, :enabled_plugins_file])
    {:ok, plugins_dir} = :rabbit_misc.rpc_call(node,
                                               :application, :get_env,
                                               [:rabbit, :plugins_dir])
    rabbitmq_home = :rabbit_misc.rpc_call(node, :code, :lib_dir, [:rabbit])

    IO.puts("plugins disable tests default env: enabled plugins = #{plugins_file}, plugins dir = #{plugins_dir}, RabbitMQ home directory = #{rabbitmq_home}")

    {:ok, [enabled_plugins]} = :file.consult(plugins_file)
    IO.puts("plugins disable tests will assume tnat #{Enum.join(enabled_plugins, ",")} is the list of enabled plugins to revert to")

    opts = %{enabled_plugins_file: plugins_file,
             plugins_dir: plugins_dir,
             rabbitmq_home: rabbitmq_home,
             online: false, offline: false,
             all: false}

    on_exit(fn ->
      set_enabled_plugins(enabled_plugins, :online, get_rabbit_hostname(), opts)
    end)

    {:ok, opts: opts}
  end

  setup context do
    set_enabled_plugins([:rabbitmq_stomp, :rabbitmq_federation],
                        :online,
                        get_rabbit_hostname(),
                        context[:opts])


    {
      :ok,
      opts: Map.merge(context[:opts], %{
              node: get_rabbit_hostname(),
              timeout: 1000
            })
    }
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

  test "validate_execution_environment: specifying a non-existent enabled_plugins_file is fine", context do
    assert @command.validate_execution_environment(["a"], Map.merge(context[:opts], %{enabled_plugins_file: "none"})) == :ok
  end

  test "validate_execution_environment: specifying a non-existent plugins_dir is reported as an error", context do
    assert @command.validate_execution_environment(["a"], Map.merge(context[:opts], %{plugins_dir: "none"})) ==
      {:validation_failure, :plugins_dir_does_not_exist}
  end

  test "node is inaccessible, writes out enabled plugins file and returns implicitly enabled plugin list", context do
    assert {:stream, test_stream} =
      @command.run(["rabbitmq_stomp"], Map.merge(context[:opts], %{node: :nonode}))
    assert [[:rabbitmq_federation],
            %{mode: :offline, disabled: [:rabbitmq_stomp], set: [:rabbitmq_federation]}] ==
      Enum.to_list(test_stream)
    assert {:ok, [[:rabbitmq_federation]]} == :file.consult(context[:opts][:enabled_plugins_file])
    assert [:amqp_client, :rabbitmq_federation, :rabbitmq_stomp] ==
           Enum.sort(:rabbit_misc.rpc_call(context[:opts][:node], :rabbit_plugins, :active, []))
  end

  test "in offline mode, writes out enabled plugins and reports implicitly enabled plugin list", context do
    assert {:stream, test_stream} = @command.run(["rabbitmq_stomp"], Map.merge(context[:opts], %{offline: true, online: false}))
    assert [[:rabbitmq_federation],
            %{mode: :offline, disabled: [:rabbitmq_stomp], set: [:rabbitmq_federation]}] == Enum.to_list(test_stream)
    assert {:ok, [[:rabbitmq_federation]]} == :file.consult(context[:opts][:enabled_plugins_file])
    assert [:amqp_client, :rabbitmq_federation, :rabbitmq_stomp] ==
           Enum.sort(:rabbit_misc.rpc_call(context[:opts][:node], :rabbit_plugins, :active, []))
  end

  test "in offline mode, removes implicitly enabled plugins when the last explicitly enabled one is removed", context do
    assert {:stream, test_stream0} =
      @command.run(["rabbitmq_federation"], Map.merge(context[:opts], %{offline: true, online: false}))
    assert [[:rabbitmq_stomp],
            %{mode: :offline, disabled: [:rabbitmq_federation], set: [:rabbitmq_stomp]}] == Enum.to_list(test_stream0)
    assert {:ok, [[:rabbitmq_stomp]]} == :file.consult(context[:opts][:enabled_plugins_file])

    assert {:stream, test_stream1} =
      @command.run(["rabbitmq_stomp"], Map.merge(context[:opts], %{offline: true, online: false}))
    assert [[],
            %{mode: :offline, disabled: [:rabbitmq_stomp], set: []}] ==
      Enum.to_list(test_stream1)
    assert {:ok, [[]]} = :file.consult(context[:opts][:enabled_plugins_file])
  end

  test "updates plugin list and stops disabled plugins", context do
    assert {:stream, test_stream0} =
      @command.run(["rabbitmq_stomp"], context[:opts])
    assert [[:rabbitmq_federation],
            %{mode: :online,
              started: [], stopped: [:rabbitmq_stomp],
              disabled: [:rabbitmq_stomp],
              set: [:rabbitmq_federation]}] ==
      Enum.to_list(test_stream0)
    assert {:ok, [[:rabbitmq_federation]]} == :file.consult(context[:opts][:enabled_plugins_file])
    assert [:amqp_client, :rabbitmq_federation] ==
           Enum.sort(:rabbit_misc.rpc_call(context[:opts][:node], :rabbit_plugins, :active, []))

    assert {:stream, test_stream1} =
      @command.run(["rabbitmq_federation"], context[:opts])
    assert [[],
            %{mode: :online,
              started: [], stopped: [:rabbitmq_federation],
              disabled: [:rabbitmq_federation],
              set: []}] ==
      Enum.to_list(test_stream1)
    assert {:ok, [[]]} == :file.consult(context[:opts][:enabled_plugins_file])
    assert Enum.empty?(Enum.sort(:rabbit_misc.rpc_call(context[:opts][:node], :rabbit_plugins, :active, [])))
  end

  test "can disable multiple plugins at once", context do
    assert {:stream, test_stream} = @command.run(["rabbitmq_stomp", "rabbitmq_federation"], context[:opts])
    [[], m0] = Enum.to_list(test_stream)
    m1 = m0 |> Map.update!(:stopped, &Enum.sort/1)
            |> Map.update!(:disabled, &Enum.sort/1)
    expected_list = Enum.sort([:rabbitmq_federation, :rabbitmq_stomp])
    assert [[],
            %{mode: :online,
              started: [],
              stopped: expected_list,
              disabled: expected_list,
              set: []}
            ] == [[], m1]
    assert {:ok, [[]]} == :file.consult(context[:opts][:enabled_plugins_file])
    assert Enum.empty?(Enum.sort(:rabbit_misc.rpc_call(context[:opts][:node], :rabbit_plugins, :active, [])))
  end

  test "disabling a dependency disables all plugins that depend on it", context do
    assert {:stream, test_stream} = @command.run(["amqp_client"], context[:opts])
    [[], m0] = Enum.to_list(test_stream)
    m1 = m0 |> Map.update!(:stopped, &Enum.sort/1)
            |> Map.update!(:disabled, &Enum.sort/1)

    expected_list = Enum.sort([:rabbitmq_federation, :rabbitmq_stomp])
    assert [[],
            %{mode: :online,
              started: [],
              stopped: expected_list,
              disabled: expected_list,
              set: []}
            ] == [[], m1]

    assert {:ok, [[]]} == :file.consult(context[:opts][:enabled_plugins_file])
    assert Enum.empty?(Enum.sort(:rabbit_misc.rpc_call(context[:opts][:node], :rabbit_plugins, :active, [])))
  end

  test "formats enabled plugins mismatch errors", context do
    err = {:enabled_plugins_mismatch, '/tmp/a/cli/path', '/tmp/a/server/path'}
    assert {:error, ExitCodes.exit_dataerr(),
            "Could not update enabled plugins file at /tmp/a/cli/path: target node #{context[:opts][:node]} uses a different path (/tmp/a/server/path)"}
      == @command.output({:error, err}, context[:opts])
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
