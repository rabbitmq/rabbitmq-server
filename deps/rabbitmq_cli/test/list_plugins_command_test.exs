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
    {:ok, rabbitmq_home} = :rabbit_misc.rpc_call(node, :file, :get_cwd, [])

    :erlang.disconnect_node(node)
    :net_kernel.stop()

    {:ok, opts: %{enabled_plugins_file: plugins_file,
                  plugins_dir: plugins_dir,
                  rabbitmq_home: rabbitmq_home,
                  minimal: false, verbose: false,
                  enabled: false, implicitly_enabled: false}}
  end

  setup context do
    RabbitMQ.CLI.Distribution.start()
    :net_kernel.connect_node(get_rabbit_hostname)

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


  test "validate: specifying non existent enabled_plugins_file is reported as an error", context do
    assert @command.validate(["a"], Map.merge(context[:opts], %{enabled_plugins_file: "none"})) ==
      {:validation_failure, :plugins_file_not_exists}
  end

  test "validate: specifying non existent plugins_dir is reported as an error", context do
    assert @command.validate(["a"], Map.merge(context[:opts], %{plugins_dir: "none"})) ==
      {:validation_failure, :plugins_dir_not_exists}
  end

  test "validate: failure to load rabbit application is reported as an error", context do
    assert {:validation_failure, {:unable_to_load_rabbit, _}} =
      @command.validate(["a"], Map.delete(context[:opts], :rabbitmq_home))
  end

  test "will report list of plugins from file for stopped node", context do
    assert @command.run([".*"], Map.merge(context[:opts], %{node: :nonode, minimal: true})) ==
      %{status: :node_down, plugins: [:amqp_client, :rabbitmq_federation, :rabbitmq_metronome]} 
  end

  test "will report list of started plugins for started node", context do
    node = context[:opts][:node]
    :ok = :rabbit_misc.rpc_call(node, :application, :stop, [:rabbitmq_metronome])
    assert %{status: :running,
             plugins: [%{name: :amqp_client, enabled: :enabled, running: true}, 
                       %{name: :rabbitmq_federation, enabled: :enabled, running: true},
                       %{name: :rabbitmq_metronome, enabled: :enabled, running: false}]} =
      @command.run([".*"], context[:opts])
  end



end
