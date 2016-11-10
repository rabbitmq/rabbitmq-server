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

defmodule SetPluginsCommandTest do
  use ExUnit.Case, async: false
  import TestHelper

  @command RabbitMQ.CLI.Plugins.Commands.SetCommand
  @vhost "test1"
  @user "guest"
  @root   "/"
  @default_timeout :infinity


  #RABBITMQ_PLUGINS_DIR=~/dev/master/deps RABBITMQ_ENABLED_PLUGINS_FILE=/var/folders/cl/jnydxpf92rg76z05m12hlly80000gq/T/rabbitmq-test-instances/rabbit/enabled_plugins RABBITMQ_HOME=~/dev/master/deps/rabbit ./rabbitmq-plugins list_plugins

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
    rabbitmq_home = :rabbit_misc.rpc_call(node, :code, :lib_dir, [:rabbit])

    {:ok, [enabled_plugins]} = :file.consult(plugins_file)

    opts = %{enabled_plugins_file: plugins_file,
             plugins_dir: plugins_dir,
             rabbitmq_home: rabbitmq_home,
             online: true, offline: false}

    on_exit(fn ->
      set_enabled_plugins(get_rabbit_hostname,enabled_plugins,opts)
    end)

    :erlang.disconnect_node(node)
    #

    {:ok, opts: opts}
  end

  setup context do
    :net_kernel.connect_node(get_rabbit_hostname)
    set_enabled_plugins(get_rabbit_hostname,
                        [:rabbitmq_stomp, :rabbitmq_federation],
                        context[:opts])

    on_exit([], fn ->
      :erlang.disconnect_node(get_rabbit_hostname)
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
      @command.validate([], Map.merge(context[:opts], %{online: true, offline: true}))
    )
  end

  test "validate: not specifying enabled_plugins_file is reported as an error", context do
    assert @command.validate([], Map.delete(context[:opts], :enabled_plugins_file)) ==
      {:validation_failure, :no_plugins_file}
  end

  test "validate: not specifying plugins_dir is reported as an error", context do
    assert @command.validate([], Map.delete(context[:opts], :plugins_dir)) ==
      {:validation_failure, :no_plugins_dir}
  end


  test "validate: specifying non existent enabled_plugins_file is fine", context do
    assert @command.validate([], Map.merge(context[:opts], %{enabled_plugins_file: "none"})) ==
      :ok
  end

  test "validate: specifying non existent plugins_dir is reported as an error", context do
    assert @command.validate([], Map.merge(context[:opts], %{plugins_dir: "none"})) ==
      {:validation_failure, :plugins_dir_does_not_exist}
  end

  test "validate: failure to load rabbit application is reported as an error", context do
    assert {:validation_failure, {:unable_to_load_rabbit, _}} =
      @command.validate([], Map.delete(context[:opts], :rabbitmq_home))
  end

  test "will write enabled plugins file if node is unaccessible and report implicitly enabled list", context do
    assert %{mode: :offline, set: [:amqp_client, :rabbitmq_stomp]} =
           @command.run(["rabbitmq_stomp"], Map.merge(context[:opts], %{node: :nonode}))
    assert {:ok, [[:rabbitmq_stomp]]} = :file.consult(context[:opts][:enabled_plugins_file])
    assert [:amqp_client, :rabbitmq_federation, :rabbitmq_stomp] =
           Enum.sort(:rabbit_misc.rpc_call(context[:opts][:node], :rabbit_plugins, :active, []))
  end

  test "will write enabled plugins in offline mode and report implicitly enabled list", context do
    assert %{mode: :offline, set: [:amqp_client, :rabbitmq_stomp]} =
           @command.run(["rabbitmq_stomp"], Map.merge(context[:opts], %{offline: true, online: false}))
    assert {:ok, [[:rabbitmq_stomp]]} = :file.consult(context[:opts][:enabled_plugins_file])
    assert [:amqp_client, :rabbitmq_federation, :rabbitmq_stomp] =
           Enum.sort(:rabbit_misc.rpc_call(context[:opts][:node], :rabbit_plugins, :active, []))
  end

  test "will update list of plugins and start/stop enabled/disabled plugins", context do
    assert %{mode: :online,
             started: [], stopped: [:rabbitmq_federation],
             set: [:amqp_client, :rabbitmq_stomp]} =
           @command.run(["rabbitmq_stomp"], context[:opts])
    assert {:ok, [[:rabbitmq_stomp]]} = :file.consult(context[:opts][:enabled_plugins_file])
    assert [:amqp_client, :rabbitmq_stomp] =
           Enum.sort(:rabbit_misc.rpc_call(context[:opts][:node], :rabbit_plugins, :active, []))

    assert %{mode: :online,
             started: [:rabbitmq_federation], stopped: [:rabbitmq_stomp],
             set: [:amqp_client, :rabbitmq_federation]} =
           @command.run(["rabbitmq_federation"], context[:opts])
    assert {:ok, [[:rabbitmq_federation]]} = :file.consult(context[:opts][:enabled_plugins_file])
    assert [:amqp_client, :rabbitmq_federation] =
           Enum.sort(:rabbit_misc.rpc_call(context[:opts][:node], :rabbit_plugins, :active, []))
  end

  test "can disable all plugins", context do
    assert %{mode: :online,
             started: [], stopped: [:amqp_client, :rabbitmq_federation, :rabbitmq_stomp],
             set: []} =
           @command.run([], context[:opts])
    assert {:ok, [[]]} = :file.consult(context[:opts][:enabled_plugins_file])
    assert [] = Enum.sort(:rabbit_misc.rpc_call(context[:opts][:node], :rabbit_plugins, :active, []))
  end

  test "can set multiple plugins", context do
    set_enabled_plugins(get_rabbit_hostname,[],context[:opts])
    assert %{mode: :online,
             started: [:amqp_client, :rabbitmq_federation, :rabbitmq_stomp], stopped: [],
             set: [:amqp_client, :rabbitmq_federation, :rabbitmq_stomp]} =
           @command.run(["rabbitmq_federation", "rabbitmq_stomp"], context[:opts])
    assert {:ok, [[:rabbitmq_federation, :rabbitmq_stomp]]} =
           :file.consult(context[:opts][:enabled_plugins_file])
    assert [:amqp_client, :rabbitmq_federation, :rabbitmq_stomp] =
           Enum.sort(:rabbit_misc.rpc_call(context[:opts][:node], :rabbit_plugins, :active, []))
  end

end
