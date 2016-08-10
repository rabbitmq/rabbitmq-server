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

defmodule EnablePluginsCommandTest do
  use ExUnit.Case, async: false
  import TestHelper

  alias RabbitMQ.CLI.Plugins.Helpers, as: PluginHelpers

  @command RabbitMQ.CLI.Plugins.Commands.EnableCommand
  @vhost "test1"
  @user "guest"
  @root   "/"
  @default_timeout :infinity

  setup_all do
    RabbitMQ.CLI.Distribution.start()
    node = get_rabbit_hostname
    :net_kernel.connect_node(node)
    {:ok, plugins_file} = :rabbit_misc.rpc_call(node,
                                                :application, :get_env,
                                                [:rabbit, :enabled_plugins_file])
    {:ok, plugins_dir} = :rabbit_misc.rpc_call(node,
                                               :application, :get_env,
                                               [:rabbit, :plugins_dir])
    {:ok, rabbitmq_home} = :rabbit_misc.rpc_call(node, :file, :get_cwd, [])

    {:ok, [enabled_plugins]} = :file.consult(plugins_file)

    opts = %{enabled_plugins_file: plugins_file,
             plugins_dir: plugins_dir,
             rabbitmq_home: rabbitmq_home,
             online: true, offline: false}

    on_exit(fn ->
      set_enabled_plugins(get_rabbit_hostname, enabled_plugins, opts)
    end)

    :erlang.disconnect_node(node)
    :net_kernel.stop()

    {:ok, opts: opts}
  end

  setup context do
    RabbitMQ.CLI.Distribution.start()
    :net_kernel.connect_node(get_rabbit_hostname)
    set_enabled_plugins(get_rabbit_hostname,
                        [:rabbitmq_metronome, :rabbitmq_federation],
                        context[:opts])

    on_exit([], fn ->
      :erlang.disconnect_node(get_rabbit_hostname)
      :net_kernel.stop()
    end)

    {
      :ok,
      opts: Map.merge(context[:opts], %{
              node: get_rabbit_hostname,
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
      {:validation_failure, :not_enough_arguments},
      @command.validate([], Map.merge(context[:opts], %{online: true, offline: false}))
    )
  end

  test "validate: not specifying enabled_plugins_file is reported as an error", context do
    assert @command.validate(["a"], Map.delete(context[:opts], :enabled_plugins_file)) ==
      {:validation_failure, :no_plugins_file}
  end

  test "validate: not specifying plugins_dir is reported as an error", context do
    assert @command.validate(["a"], Map.delete(context[:opts], :plugins_dir)) ==
      {:validation_failure, :no_plugins_dir}
  end


  test "validate: specifying non existent enabled_plugins_file is reported as an error", context do
    assert @command.validate(["a"], Map.merge(context[:opts], %{enabled_plugins_file: "none"})) ==
      {:validation_failure, :enabled_plugins_file_does_not_exist}
  end

  test "validate: specifying non existent plugins_dir is reported as an error", context do
    assert @command.validate(["a"], Map.merge(context[:opts], %{plugins_dir: "none"})) ==
      {:validation_failure, :plugins_dir_does_not_exist}
  end

  test "validate: failure to load rabbit application is reported as an error", context do
    assert {:validation_failure, {:unable_to_load_rabbit, _}} =
      @command.validate(["a"], Map.delete(context[:opts], :rabbitmq_home))
  end

  test "will write enabled plugins file if node is unaccessible and report implicitly enabled list", context do
    # Clear plugins file
    PluginHelpers.set_enabled_plugins([], :offline, :nonode, context[:opts])

    assert %{mode: :offline, enabled: [:amqp_client, :rabbitmq_metronome]} =
           @command.run(["rabbitmq_metronome"], Map.merge(context[:opts], %{node: :nonode}))
    assert {:ok, [[:rabbitmq_metronome]]} = :file.consult(context[:opts][:enabled_plugins_file])
    assert [:amqp_client, :rabbitmq_federation, :rabbitmq_metronome] =
           currently_active_plugins(context)
  end

  test "will write enabled plugins in offline mode and report implicitly enabled list", context do
    # Clear plugins file
    PluginHelpers.set_enabled_plugins([], :offline, :nonode, context[:opts])

    assert %{mode: :offline, enabled: [:amqp_client, :rabbitmq_metronome]} =
           @command.run(["rabbitmq_metronome"], Map.merge(context[:opts], %{offline: true, online: false}))
    assert {:ok, [[:rabbitmq_metronome]]} = :file.consult(context[:opts][:enabled_plugins_file])

    assert_equal_sets(
      [:amqp_client, :rabbitmq_federation, :rabbitmq_metronome],
      currently_active_plugins(context))
  end

  test "will add additional plugin to already enabled", context do
    # Clear plugins file
    PluginHelpers.set_enabled_plugins([], :offline, :nonode, context[:opts])

    assert %{mode: :offline, enabled: [:amqp_client, :rabbitmq_metronome]} =
           @command.run(["rabbitmq_metronome"], Map.merge(context[:opts], %{offline: true, online: false}))
    assert {:ok, [[:rabbitmq_metronome]]} = :file.consult(context[:opts][:enabled_plugins_file])

    assert %{mode: :offline, enabled: [:amqp_client, :rabbitmq_federation, :rabbitmq_metronome]} =
           @command.run(["rabbitmq_federation"], Map.merge(context[:opts], %{offline: true, online: false}))
    {:ok, [xs]} = :file.consult(context[:opts][:enabled_plugins_file])

    assert_equal_sets([:rabbitmq_metronome, :rabbitmq_federation], xs)
  end

  test "will update list of plugins and start enabled plugins", context do
    # Clear plugins file and stop all plugins
    PluginHelpers.set_enabled_plugins([], :online, context[:opts][:node], context[:opts])

    assert %{mode: :online,
             started: [:amqp_client, :rabbitmq_metronome], stopped: [],
             enabled: [:amqp_client, :rabbitmq_metronome]} =
           @command.run(["rabbitmq_metronome"], context[:opts])
    assert {:ok, [xs]} = :file.consult(context[:opts][:enabled_plugins_file])
    
    assert_equal_sets([:rabbitmq_metronome], xs)
    assert_equal_sets([:amqp_client, :rabbitmq_metronome], currently_active_plugins(context))

    assert %{mode: :online,
             started: [:rabbitmq_federation], stopped: [],
             enabled: [:amqp_client, :rabbitmq_federation, :rabbitmq_metronome]} =
           @command.run(["rabbitmq_federation"], context[:opts])
    assert {:ok, [xs]} = :file.consult(context[:opts][:enabled_plugins_file])

    assert_equal_sets([:rabbitmq_metronome, :rabbitmq_federation], xs)
    assert_equal_sets([:amqp_client, :rabbitmq_federation, :rabbitmq_metronome], currently_active_plugins(context))
  end

  test "can enable multiple plugins", context do
    # Clear plugins file and stop all plugins
    PluginHelpers.set_enabled_plugins([], :online, context[:opts][:node], context[:opts])

    assert %{mode: :online,
             started: [:amqp_client, :rabbitmq_federation, :rabbitmq_metronome], stopped: [],
             enabled: [:amqp_client, :rabbitmq_federation, :rabbitmq_metronome]} =
           @command.run(["rabbitmq_metronome", "rabbitmq_federation"], context[:opts])
    assert {:ok, [xs]} = :file.consult(context[:opts][:enabled_plugins_file])

    assert_equal_sets([:rabbitmq_metronome, :rabbitmq_federation], xs)
    assert_equal_sets([:amqp_client, :rabbitmq_federation, :rabbitmq_metronome], currently_active_plugins(context))
  end

  test "will not enable already enabled implicit plugin", context do
    # Clear plugins file and stop all plugins
    PluginHelpers.set_enabled_plugins([:rabbitmq_federation], :online, context[:opts][:node], context[:opts])

    assert %{mode: :online,
             started: [], stopped: [],
             enabled: [:amqp_client, :rabbitmq_federation]} =
           @command.run(["amqp_client"], context[:opts])
    assert {:ok, [[:rabbitmq_federation]]} = :file.consult(context[:opts][:enabled_plugins_file])
    assert [:amqp_client, :rabbitmq_federation] =
           currently_active_plugins(context)

  end

  defp assert_equal_sets(a, b) do
    assert MapSet.equal?(MapSet.new(a), MapSet.new(b))
  end
end
