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

defmodule ListPluginsCommandTest do
  use ExUnit.Case, async: false
  import TestHelper

  @command RabbitMQ.CLI.Plugins.Commands.ListCommand
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
             minimal: false, verbose: false,
             enabled: false, implicitly_enabled: false}

    on_exit(fn ->
      set_enabled_plugins(get_rabbit_hostname,enabled_plugins,opts)
    end)

    :erlang.disconnect_node(node)


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

  test "validate: specifying both --minimal and --verbose is reported as invalid", context do
    assert match?(
      {:validation_failure, {:bad_argument, _}},
      @command.validate([], Map.merge(context[:opts], %{minimal: true, verbose: true}))
    )
  end

  test "validate: specifying multiple patterns is reported as an error", context do
    assert @command.validate(["a", "b", "c"], context[:opts]) ==
      {:validation_failure, :too_many_args}
  end

  test "validate: not specifying enabled_plugins_file is reported as an error", context do
    assert @command.validate(["a"], Map.delete(context[:opts], :enabled_plugins_file)) ==
      {:validation_failure, :no_plugins_file}
  end

  test "validate: not specifying plugins_dir is reported as an error", context do
    assert @command.validate(["a"], Map.delete(context[:opts], :plugins_dir)) ==
      {:validation_failure, :no_plugins_dir}
  end


  test "validate: specifying non existent enabled_plugins_file is fine", context do
    assert @command.validate(["a"], Map.merge(context[:opts], %{enabled_plugins_file: "none"})) == :ok
  end

  test "validate: specifying non existent plugins_dir is reported as an error", context do
    assert @command.validate(["a"], Map.merge(context[:opts], %{plugins_dir: "none"})) ==
      {:validation_failure, :plugins_dir_does_not_exist}
  end

  test "validate: failure to load rabbit application is reported as an error", context do
    assert {:validation_failure, {:unable_to_load_rabbit, _}} =
      @command.validate(["a"], Map.delete(context[:opts], :rabbitmq_home))
  end

  test "will report list of plugins from file for stopped node", context do
    node = context[:opts][:node]
    :ok = :rabbit_misc.rpc_call(node, :application, :stop, [:rabbitmq_stomp])
    on_exit(fn ->
      :rabbit_misc.rpc_call(node, :application, :start, [:rabbitmq_stomp])
    end)
    assert %{status: :node_down,
             plugins: [%{name: :amqp_client, enabled: :implicit, running: false},
                       %{name: :rabbitmq_federation, enabled: :enabled, running: false},
                       %{name: :rabbitmq_stomp, enabled: :enabled, running: false}]} =
           @command.run([".*"], Map.merge(context[:opts], %{node: :nonode}))
  end

  test "will report list of started plugins for started node", context do
    node = context[:opts][:node]
    :ok = :rabbit_misc.rpc_call(node, :application, :stop, [:rabbitmq_stomp])
    on_exit(fn ->
      :rabbit_misc.rpc_call(node, :application, :start, [:rabbitmq_stomp])
    end)
    assert %{status: :running,
             plugins: [%{name: :amqp_client, enabled: :implicit, running: true},
                       %{name: :rabbitmq_federation, enabled: :enabled, running: true},
                       %{name: :rabbitmq_stomp, enabled: :enabled, running: false}]} =
      @command.run([".*"], context[:opts])
  end

  test "will report description and dependencies for verbose mode", context do
    assert %{status: :running,
             plugins: [%{name: :amqp_client, enabled: :implicit, running: true, description: _, dependencies: []},
                       %{name: :rabbitmq_federation, enabled: :enabled, running: true, description: _, dependencies: [:amqp_client]},
                       %{name: :rabbitmq_stomp, enabled: :enabled, running: true, description: _, dependencies: [:amqp_client]}]} =
           @command.run([".*"], Map.merge(context[:opts], %{verbose: true}))
  end

  test "will report plugin names in minimal mode", context do
    assert %{status: :running,
             plugins: [%{name: :amqp_client}, %{name: :rabbitmq_federation}, %{name: :rabbitmq_stomp}]} =
           @command.run([".*"], Map.merge(context[:opts], %{minimal: true}))
  end


  test "by default lists all plugins", context do
    set_enabled_plugins(context[:opts][:node], [:rabbitmq_federation], context[:opts])
    on_exit(fn ->
      set_enabled_plugins(context[:opts][:node], [:rabbitmq_stomp, :rabbitmq_federation], context[:opts])
    end)
    assert %{status: :running,
             plugins: [%{name: :amqp_client, enabled: :implicit, running: true},
                       %{name: :rabbitmq_federation, enabled: :enabled, running: true},
                       %{name: :rabbitmq_stomp, enabled: :not_enabled, running: false}]} =
           @command.run([".*"], context[:opts])
  end

  test "with enabled flag lists only explicitly enabled plugins", context do
    set_enabled_plugins(context[:opts][:node], [:rabbitmq_federation], context[:opts])
    on_exit(fn ->
      set_enabled_plugins(context[:opts][:node], [:rabbitmq_stomp, :rabbitmq_federation], context[:opts])
    end)
    assert %{status: :running,
             plugins: [%{name: :rabbitmq_federation, enabled: :enabled, running: true}]} =
           @command.run([".*"], Map.merge(context[:opts], %{enabled: true}))
  end

  test "with implicitly_enabled flag lists explicitly and implicitly enabled plugins", context do
    set_enabled_plugins(context[:opts][:node], [:rabbitmq_federation], context[:opts])
    on_exit(fn ->
      set_enabled_plugins(context[:opts][:node], [:rabbitmq_stomp, :rabbitmq_federation], context[:opts])
    end)
    assert %{status: :running,
             plugins: [%{name: :amqp_client, enabled: :implicit, running: true},
                       %{name: :rabbitmq_federation, enabled: :enabled, running: true}]} =
           @command.run([".*"], Map.merge(context[:opts], %{implicitly_enabled: true}))
  end

  test "will filter plugins by name with pattern provided", context do
    set_enabled_plugins(context[:opts][:node], [:rabbitmq_federation], context[:opts])
    on_exit(fn ->
      set_enabled_plugins(context[:opts][:node], [:rabbitmq_stomp, :rabbitmq_federation], context[:opts])
    end)    
    assert %{status: :running,
             plugins: [%{name: :rabbitmq_federation}]} =
           @command.run(["fede"], Map.merge(context[:opts], %{minimal: true}))
    assert %{status: :running,
             plugins: [%{name: :amqp_client}]} =
           @command.run(["^[a-z]mq"], Map.merge(context[:opts], %{minimal: true}))
    assert %{status: :running,
             plugins: [%{name: :rabbitmq_stomp}]} =
           @command.run(["stomp$"], Map.merge(context[:opts], %{minimal: true}))
  end

end
