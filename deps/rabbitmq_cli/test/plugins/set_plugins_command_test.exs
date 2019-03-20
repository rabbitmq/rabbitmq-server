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

defmodule SetPluginsCommandTest do
  use ExUnit.Case, async: false
  import TestHelper

  @command RabbitMQ.CLI.Plugins.Commands.SetCommand


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

    {:ok, [enabled_plugins]} = :file.consult(plugins_file)

    opts = %{enabled_plugins_file: plugins_file,
             plugins_dir: plugins_dir,
             rabbitmq_home: rabbitmq_home,
             online: false, offline: false}

    on_exit(fn ->
      set_enabled_plugins(enabled_plugins, :online, get_rabbit_hostname(),opts)
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
      @command.validate([], Map.merge(context[:opts], %{online: true, offline: true}))
    )
  end

  test "validate_execution_environment: not specifying enabled_plugins_file is reported as an error", context do
    assert @command.validate_execution_environment([], Map.delete(context[:opts], :enabled_plugins_file)) ==
      {:validation_failure, :no_plugins_file}
  end

  test "validate_execution_environment: not specifying plugins_dir is reported as an error", context do
    assert @command.validate_execution_environment([], Map.delete(context[:opts], :plugins_dir)) ==
      {:validation_failure, :no_plugins_dir}
  end

  test "validate_execution_environment: specifying a non-existent enabled_plugins_file is fine", context do
    assert @command.validate_execution_environment([], Map.merge(context[:opts], %{enabled_plugins_file: "none"})) ==
      :ok
  end

  test "validate_execution_environment: specifying non existent plugins_dir is reported as an error", context do
    assert @command.validate_execution_environment([], Map.merge(context[:opts], %{plugins_dir: "none"})) ==
      {:validation_failure, :plugins_dir_does_not_exist}
  end

  test "validate_execution_environment: failure to load rabbit application is reported as an error", context do
    assert {:validation_failure, {:unable_to_load_rabbit, _}} =
      @command.validate_execution_environment([], Map.delete(context[:opts], :rabbitmq_home))
  end

  test "will write enabled plugins file if node is inaccessible and report implicitly enabled list", context do
    assert {:stream, test_stream} =
      @command.run(["rabbitmq_stomp"], Map.merge(context[:opts], %{node: :nonode}))
    assert [[:rabbitmq_stomp],
            %{mode: :offline, set: [:rabbitmq_stomp]}] =
      Enum.to_list(test_stream)
    assert {:ok, [[:rabbitmq_stomp]]} = :file.consult(context[:opts][:enabled_plugins_file])
    assert [:amqp_client, :rabbitmq_federation, :rabbitmq_stomp] =
           Enum.sort(:rabbit_misc.rpc_call(context[:opts][:node], :rabbit_plugins, :active, []))
  end

  test "will write enabled plugins in offline mode and report implicitly enabled list", context do
    assert {:stream, test_stream} =
      @command.run(["rabbitmq_stomp"], Map.merge(context[:opts], %{offline: true, online: false}))
    assert [[:rabbitmq_stomp],
            %{mode: :offline, set: [:rabbitmq_stomp]}] =
      Enum.to_list(test_stream)
    assert {:ok, [[:rabbitmq_stomp]]} = :file.consult(context[:opts][:enabled_plugins_file])
    assert [:amqp_client, :rabbitmq_federation, :rabbitmq_stomp] =
           Enum.sort(:rabbit_misc.rpc_call(context[:opts][:node], :rabbit_plugins, :active, []))
  end

  test "will update list of plugins and start/stop enabled/disabled plugins", context do
    assert {:stream, test_stream0} = @command.run(["rabbitmq_stomp"], context[:opts])
    assert [[:rabbitmq_stomp],
            %{mode: :online,
              started: [], stopped: [:rabbitmq_federation],
              set: [:rabbitmq_stomp]}] =
      Enum.to_list(test_stream0)
    assert {:ok, [[:rabbitmq_stomp]]} = :file.consult(context[:opts][:enabled_plugins_file])
    assert [:amqp_client, :rabbitmq_stomp] =
           Enum.sort(:rabbit_misc.rpc_call(context[:opts][:node], :rabbit_plugins, :active, []))
    assert {:stream, test_stream1} = @command.run(["rabbitmq_federation"], context[:opts])
    assert [[:rabbitmq_federation],
            %{mode: :online,
              started: [:rabbitmq_federation], stopped: [:rabbitmq_stomp],
              set: [:rabbitmq_federation]}] =
      Enum.to_list(test_stream1)
    assert {:ok, [[:rabbitmq_federation]]} = :file.consult(context[:opts][:enabled_plugins_file])
    assert [:amqp_client, :rabbitmq_federation] =
           Enum.sort(:rabbit_misc.rpc_call(context[:opts][:node], :rabbit_plugins, :active, []))
  end

  test "can disable all plugins", context do
    assert {:stream, test_stream} = @command.run([], context[:opts])
    assert [[],
            %{mode: :online,
              started: [], stopped: [:rabbitmq_federation, :rabbitmq_stomp],
              set: []}] =
      Enum.to_list(test_stream)
    assert {:ok, [[]]} = :file.consult(context[:opts][:enabled_plugins_file])
    assert [] = Enum.sort(:rabbit_misc.rpc_call(context[:opts][:node], :rabbit_plugins, :active, []))
  end

  test "can set multiple plugins", context do
    set_enabled_plugins([], :online, get_rabbit_hostname(), context[:opts])
    assert {:stream, test_stream} =
      @command.run(["rabbitmq_federation", "rabbitmq_stomp"], context[:opts])
    assert [[:rabbitmq_federation, :rabbitmq_stomp],
            %{mode: :online,
              started: [:rabbitmq_federation, :rabbitmq_stomp],
              stopped: [],
              set: [:rabbitmq_federation, :rabbitmq_stomp]}] =
      Enum.to_list(test_stream)
    assert {:ok, [[:rabbitmq_federation, :rabbitmq_stomp]]} =
           :file.consult(context[:opts][:enabled_plugins_file])
    assert [:amqp_client, :rabbitmq_federation, :rabbitmq_stomp] =
           Enum.sort(:rabbit_misc.rpc_call(context[:opts][:node], :rabbit_plugins, :active, []))
  end

  test "run: does not enable plugins with unmet version requirements", context do
    set_enabled_plugins([], :online, context[:opts][:node], context[:opts])

    plugins_directory = fixture_plugins_path("plugins_with_version_requirements")
    opts = get_opts_with_plugins_directories(context, [plugins_directory])
    switch_plugins_directories(context[:opts][:plugins_dir], opts[:plugins_dir])

    {:stream, _} = @command.run(["mock_rabbitmq_plugin_for_3_7"], opts)
    check_plugins_enabled([:mock_rabbitmq_plugin_for_3_7], context)

    {:error, _version_error} =
      @command.run(["mock_rabbitmq_plugin_for_3_8"], opts)

    check_plugins_enabled([:mock_rabbitmq_plugin_for_3_7], context)
  end


end
