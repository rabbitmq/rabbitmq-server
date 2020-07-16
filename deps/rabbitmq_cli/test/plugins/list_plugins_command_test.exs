## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2020 VMware, Inc. or its affiliates.  All rights reserved.

defmodule ListPluginsCommandTest do
  use ExUnit.Case, async: false
  import TestHelper

  @command RabbitMQ.CLI.Plugins.Commands.ListCommand

  def reset_enabled_plugins_to_preconfigured_defaults(context) do
    set_enabled_plugins([:rabbitmq_stomp, :rabbitmq_federation],
      :online,
      get_rabbit_hostname(), context[:opts])
  end

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
    IO.puts("plugins list tests default env: enabled plugins = #{plugins_file}, plugins dir = #{plugins_dir}, RabbitMQ home directory = #{rabbitmq_home}")

    {:ok, [enabled_plugins]} = :file.consult(plugins_file)
    IO.puts("plugins list tests will assume tnat #{Enum.join(enabled_plugins, ",")} is the list of enabled plugins to revert to")

    opts = %{enabled_plugins_file: plugins_file,
             plugins_dir: plugins_dir,
             rabbitmq_home: rabbitmq_home,
             minimal: false, verbose: false,
             enabled: false, implicitly_enabled: false}

    on_exit(fn ->
      set_enabled_plugins(enabled_plugins, :online, get_rabbit_hostname(), opts)
    end)


    {:ok, opts: opts}
  end

  setup context do
    reset_enabled_plugins_to_preconfigured_defaults(context)

    {
      :ok,
      opts: Map.merge(context[:opts], %{
              node: get_rabbit_hostname(),
              timeout: 1000
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

  test "validate_execution_environment: specifying a non-existent enabled_plugins_file is fine", context do
    assert @command.validate_execution_environment(["a"], Map.merge(context[:opts], %{enabled_plugins_file: "none"})) == :ok
  end

  test "validate_execution_environment: specifying non existent plugins_dir is reported as an error", context do
    assert @command.validate_execution_environment(["a"], Map.merge(context[:opts], %{plugins_dir: "none"})) ==
      {:validation_failure, :plugins_dir_does_not_exist}
  end

  test "will report list of plugins from file for stopped node", context do
    node = context[:opts][:node]
    :ok = :rabbit_misc.rpc_call(node, :application, :stop, [:rabbitmq_stomp])
    on_exit(fn ->
      :rabbit_misc.rpc_call(node, :application, :start, [:rabbitmq_stomp])
    end)
    assert %{status: :node_down,
             plugins: [%{name: :rabbitmq_federation, enabled: :enabled, running: false},
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
             plugins: [%{name: :rabbitmq_federation, enabled: :enabled, running: true},
                       %{name: :rabbitmq_stomp, enabled: :enabled, running: false}]} =
      @command.run([".*"], context[:opts])
  end

  test "will report description and dependencies for verbose mode", context do
    assert %{status: :running,
             plugins: [%{name: :rabbitmq_federation, enabled: :enabled, running: true, description: _, dependencies: [:amqp_client]},
                       %{name: :rabbitmq_stomp, enabled: :enabled, running: true, description: _, dependencies: [:amqp_client]}]} =
           @command.run([".*"], Map.merge(context[:opts], %{verbose: true}))
  end

  test "will report plugin names in minimal mode", context do
    assert %{status: :running,
             plugins: [%{name: :rabbitmq_federation}, %{name: :rabbitmq_stomp}]} =
           @command.run([".*"], Map.merge(context[:opts], %{minimal: true}))
  end

  test "by default lists all plugins", context do
    set_enabled_plugins([:rabbitmq_federation], :online, context[:opts][:node], context[:opts])
    on_exit(fn ->
      set_enabled_plugins([:rabbitmq_stomp, :rabbitmq_federation], :online, context[:opts][:node], context[:opts])
    end)
    assert %{status: :running,
             plugins: [%{name: :rabbitmq_federation, enabled: :enabled, running: true},
                       %{name: :rabbitmq_stomp, enabled: :not_enabled, running: false}]} =
           @command.run([".*"], context[:opts])
  end

  test "with enabled flag lists only explicitly enabled plugins", context do
    set_enabled_plugins([:rabbitmq_federation], :online, context[:opts][:node], context[:opts])
    on_exit(fn ->
      set_enabled_plugins([:rabbitmq_stomp, :rabbitmq_federation], :online, context[:opts][:node], context[:opts])
    end)
    assert %{status: :running,
             plugins: [%{name: :rabbitmq_federation, enabled: :enabled, running: true}]} =
           @command.run([".*"], Map.merge(context[:opts], %{enabled: true}))
  end

  test "with implicitly_enabled flag lists explicitly and implicitly enabled plugins", context do
    set_enabled_plugins([:rabbitmq_federation], :online, context[:opts][:node], context[:opts])
    on_exit(fn ->
      set_enabled_plugins([:rabbitmq_stomp, :rabbitmq_federation], :online, context[:opts][:node], context[:opts])
    end)
    assert %{status: :running,
             plugins: [%{name: :rabbitmq_federation, enabled: :enabled, running: true}]} =
           @command.run([".*"], Map.merge(context[:opts], %{implicitly_enabled: true}))
  end

  test "will filter plugins by name with pattern provided", context do
    set_enabled_plugins([:rabbitmq_federation], :online, context[:opts][:node], context[:opts])
    on_exit(fn ->
      set_enabled_plugins([:rabbitmq_stomp, :rabbitmq_federation], :online, context[:opts][:node], context[:opts])
    end)
    assert %{status: :running,
             plugins: [%{name: :rabbitmq_federation}]} =
           @command.run(["fede"], Map.merge(context[:opts], %{minimal: true}))
    assert %{status: :running,
             plugins: [%{name: :rabbitmq_stomp}]} =
           @command.run(["stomp$"], Map.merge(context[:opts], %{minimal: true}))
  end

  test "validate: validation is OK when we use multiple plugins directories, one of them does not exist", context do
    opts = get_opts_with_non_existing_plugins_directory(context)
    assert @command.validate([], opts) == :ok
  end

  test "validate: validation is OK when we use multiple plugins directories, directories do exist", context do
    opts = get_opts_with_existing_plugins_directory(context)
    assert @command.validate([], opts) == :ok
  end

  test "should succeed when using multiple plugins directories, one of them does not exist", context do
    opts = get_opts_with_non_existing_plugins_directory(context)
    assert %{status: :running,
               plugins: [%{name: :rabbitmq_federation}, %{name: :rabbitmq_stomp}]} =
             @command.run([".*"], Map.merge(opts, %{minimal: true}))
  end


  test "should succeed when using multiple plugins directories, directories do exist and do contain plugins", context do
    opts = get_opts_with_existing_plugins_directory(context)
    assert %{status: :running,
               plugins: [%{name: :rabbitmq_federation}, %{name: :rabbitmq_stomp}]} =
             @command.run([".*"], Map.merge(opts, %{minimal: true}))
  end

  test "should list plugins when using multiple plugins directories", context do
    plugins_directory = fixture_plugins_path("plugins-subdirectory-01")
    opts = get_opts_with_plugins_directories(context, [plugins_directory])
    switch_plugins_directories(context[:opts][:plugins_dir], opts[:plugins_dir])
    reset_enabled_plugins_to_preconfigured_defaults(context)
    assert %{status: :running,
                 plugins: [%{name: :mock_rabbitmq_plugins_01}, %{name: :mock_rabbitmq_plugins_02},
                           %{name: :rabbitmq_federation}, %{name: :rabbitmq_stomp}]} =
               @command.run([".*"], Map.merge(opts, %{minimal: true}))
  end

  test "will report list of plugins with latest version picked", context do
    plugins_directory_01 = fixture_plugins_path("plugins-subdirectory-01")
    plugins_directory_02 = fixture_plugins_path("plugins-subdirectory-02")
    opts = get_opts_with_plugins_directories(context, [plugins_directory_01, plugins_directory_02])
    switch_plugins_directories(context[:opts][:plugins_dir], opts[:plugins_dir])
    reset_enabled_plugins_to_preconfigured_defaults(context)
    assert %{status: :running,
             plugins: [%{name: :mock_rabbitmq_plugins_01, enabled: :not_enabled, running: false, version: '0.2.0'},
                       %{name: :mock_rabbitmq_plugins_02, enabled: :not_enabled, running: false, version: '0.2.0'},
                       %{name: :rabbitmq_federation, enabled: :enabled, running: true},
                       %{name: :rabbitmq_stomp, enabled: :enabled, running: true}]} =
      @command.run([".*"], opts)
  end

  test "will report both running and pending upgrade versions", context do
    plugins_directory_01 = fixture_plugins_path("plugins-subdirectory-01")
    plugins_directory_02 = fixture_plugins_path("plugins-subdirectory-02")
    opts = get_opts_with_plugins_directories(context, [plugins_directory_01])
    switch_plugins_directories(context[:opts][:plugins_dir], opts[:plugins_dir])
    set_enabled_plugins([:mock_rabbitmq_plugins_02, :rabbitmq_federation, :rabbitmq_stomp],
                        :online, get_rabbit_hostname(), opts)
    assert %{status: :running,
             plugins: [%{name: :mock_rabbitmq_plugins_01, enabled: :not_enabled, running: false, version: '0.2.0'},
                       %{name: :mock_rabbitmq_plugins_02, enabled: :enabled, running: true, version: '0.1.0', running_version: '0.1.0'},
                       %{name: :rabbitmq_federation, enabled: :enabled, running: true},
                       %{name: :rabbitmq_stomp, enabled: :enabled, running: true}]} =
      @command.run([".*"], opts)
    opts = get_opts_with_plugins_directories(context, [plugins_directory_01, plugins_directory_02])
    switch_plugins_directories(context[:opts][:plugins_dir], opts[:plugins_dir])
    assert %{status: :running,
             plugins: [%{name: :mock_rabbitmq_plugins_01, enabled: :not_enabled, running: false, version: '0.2.0'},
                       %{name: :mock_rabbitmq_plugins_02, enabled: :enabled, running: true, version: '0.2.0', running_version: '0.1.0'},
                       %{name: :rabbitmq_federation, enabled: :enabled, running: true},
                       %{name: :rabbitmq_stomp, enabled: :enabled, running: true}]} =
      @command.run([".*"], opts)
  end
end
