## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2023 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries.  All rights reserved.

defmodule ListPluginsCommandTest do
  use ExUnit.Case, async: false
  import TestHelper

  @command RabbitMQ.CLI.Plugins.Commands.ListCommand

  def reset_enabled_plugins_to_preconfigured_defaults(context) do
    set_enabled_plugins(
      [:rabbitmq_stomp, :rabbitmq_federation],
      :online,
      get_rabbit_hostname(),
      context[:opts]
    )
  end

  setup_all do
    RabbitMQ.CLI.Core.Distribution.start()
    node = get_rabbit_hostname()

    {:ok, plugins_file} =
      :rabbit_misc.rpc_call(node, :application, :get_env, [:rabbit, :enabled_plugins_file])

    {:ok, plugins_dir} =
      :rabbit_misc.rpc_call(node, :application, :get_env, [:rabbit, :plugins_dir])

    rabbitmq_home = :rabbit_misc.rpc_call(node, :code, :lib_dir, [:rabbit])

    IO.puts(
      "plugins list tests default env: enabled plugins = #{plugins_file}, plugins dir = #{plugins_dir}, RabbitMQ home directory = #{rabbitmq_home}"
    )

    {:ok, [enabled_plugins]} = :file.consult(plugins_file)

    IO.puts(
      "plugins list tests will assume tnat #{Enum.join(enabled_plugins, ",")} is the list of enabled plugins to revert to"
    )

    opts = %{
      enabled_plugins_file: plugins_file,
      plugins_dir: plugins_dir,
      rabbitmq_home: rabbitmq_home,
      minimal: false,
      verbose: false,
      enabled: false,
      implicitly_enabled: false
    }

    on_exit(fn ->
      set_enabled_plugins(enabled_plugins, :online, get_rabbit_hostname(), opts)
    end)

    {:ok, opts: opts}
  end

  setup context do
    reset_enabled_plugins_to_preconfigured_defaults(context)

    {
      :ok,
      opts:
        Map.merge(context[:opts], %{
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

  test "validate: succeeds when multiple plugins directories are used, and one of them does not exist",
       context do
    opts = get_opts_with_non_existing_plugins_directory(context)
    assert @command.validate([], opts) == :ok
  end

  test "validate: succeeds when multiple plugins directories are used and all of them exist",
       context do
    opts = get_opts_with_existing_plugins_directory(context)
    assert @command.validate([], opts) == :ok
  end

  test "validate_execution_environment: specifying a non-existent enabled_plugins_file is fine",
       context do
    assert @command.validate_execution_environment(
             ["a"],
             Map.merge(context[:opts], %{enabled_plugins_file: "none"})
           ) == :ok
  end

  test "validate_execution_environment: specifying non existent plugins_dir is reported as an error",
       context do
    assert @command.validate_execution_environment(
             ["a"],
             Map.merge(context[:opts], %{plugins_dir: "none"})
           ) ==
             {:validation_failure, :plugins_dir_does_not_exist}
  end

  test "run: reports list of plugins from file for stopped node", context do
    reset_enabled_plugins_to_preconfigured_defaults(context)

    node = context[:opts][:node]
    :ok = :rabbit_misc.rpc_call(node, :application, :stop, [:rabbitmq_stomp])

    on_exit(fn ->
      :rabbit_misc.rpc_call(node, :application, :start, [:rabbitmq_stomp])
    end)

    expected_plugins = [
      %{name: :rabbitmq_federation, enabled: :enabled, running: false},
      %{name: :rabbitmq_stomp, enabled: :enabled, running: false}
    ]

    %{
      plugins: actual_plugins
    } = @command.run([".*"], Map.merge(context[:opts], %{node: :nonode}))

    assert_plugin_states(actual_plugins, expected_plugins)
  end

  test "run: reports list of started plugins for started node", context do
    reset_enabled_plugins_to_preconfigured_defaults(context)

    node = context[:opts][:node]
    :ok = :rabbit_misc.rpc_call(node, :application, :stop, [:rabbitmq_stomp])

    on_exit(fn ->
      :rabbit_misc.rpc_call(node, :application, :start, [:rabbitmq_stomp])
    end)

    expected_plugins = [
      %{name: :rabbitmq_federation, enabled: :enabled, running: true},
      %{name: :rabbitmq_stomp, enabled: :enabled, running: false}
    ]

    %{
      status: :running,
      plugins: actual_plugins
    } = @command.run([".*"], context[:opts])

    assert_plugin_states(actual_plugins, expected_plugins)
  end

  test "run: in verbose mode, reports plugin description and dependencies", context do
    reset_enabled_plugins_to_preconfigured_defaults(context)

    expected_plugins = [
      %{
        name: :rabbitmq_federation,
        enabled: :enabled,
        running: true,
        dependencies: [:amqp_client]
      },
      %{
        name: :rabbitmq_stomp,
        enabled: :enabled,
        running: true,
        dependencies: [:amqp_client]
      }
    ]

    %{
      plugins: actual_plugins
    } = @command.run([".*"], Map.merge(context[:opts], %{verbose: true}))

    assert_plugin_states(actual_plugins, expected_plugins)
  end

  test "run: reports plugin names in minimal mode", context do
    reset_enabled_plugins_to_preconfigured_defaults(context)

    expected_plugins = [%{name: :rabbitmq_federation}, %{name: :rabbitmq_stomp}]

    %{status: :running, plugins: actual_plugins} =
      @command.run([".*"], Map.merge(context[:opts], %{minimal: true}))

    assert_plugin_states(actual_plugins, expected_plugins)
  end

  test "run: by default lists all plugins", context do
    reset_enabled_plugins_to_preconfigured_defaults(context)
    set_enabled_plugins([:rabbitmq_federation], :online, context[:opts][:node], context[:opts])

    on_exit(fn ->
      set_enabled_plugins(
        [:rabbitmq_stomp, :rabbitmq_federation],
        :online,
        context[:opts][:node],
        context[:opts]
      )
    end)

    expected_plugins = [
      %{name: :rabbitmq_federation, enabled: :enabled, running: true},
      %{name: :rabbitmq_stomp, enabled: :not_enabled, running: false}
    ]

    %{
      plugins: actual_plugins
    } = @command.run([".*"], context[:opts])

    assert_plugin_states(actual_plugins, expected_plugins)
  end

  test "run: with --enabled flag, lists only explicitly enabled plugins", context do
    reset_enabled_plugins_to_preconfigured_defaults(context)
    set_enabled_plugins([:rabbitmq_federation], :online, context[:opts][:node], context[:opts])

    on_exit(fn ->
      set_enabled_plugins(
        [:rabbitmq_stomp, :rabbitmq_federation],
        :online,
        context[:opts][:node],
        context[:opts]
      )
    end)

    expected_plugins = [%{name: :rabbitmq_federation, enabled: :enabled, running: true}]

    %{
      status: :running,
      plugins: actual_plugins
    } = @command.run([".*"], Map.merge(context[:opts], %{enabled: true}))

    assert_plugin_states(actual_plugins, expected_plugins)
  end

  test "run: with --implicitly_enabled flag lists explicitly and implicitly enabled plugins",
       context do
    reset_enabled_plugins_to_preconfigured_defaults(context)
    set_enabled_plugins([:rabbitmq_federation], :online, context[:opts][:node], context[:opts])

    on_exit(fn ->
      set_enabled_plugins(
        [:rabbitmq_stomp, :rabbitmq_federation],
        :online,
        context[:opts][:node],
        context[:opts]
      )
    end)

    expected_plugins = [%{name: :rabbitmq_federation, enabled: :enabled, running: true}]

    %{
      status: :running,
      plugins: actual_plugins
    } = @command.run([".*"], Map.merge(context[:opts], %{implicitly_enabled: true}))

    assert_plugin_states(actual_plugins, expected_plugins)
  end

  test "run: filters plugins by name with pattern provided", context do
    reset_enabled_plugins_to_preconfigured_defaults(context)
    set_enabled_plugins([:rabbitmq_federation], :online, context[:opts][:node], context[:opts])

    on_exit(fn ->
      set_enabled_plugins(
        [:rabbitmq_stomp, :rabbitmq_federation],
        :online,
        context[:opts][:node],
        context[:opts]
      )
    end)

    %{status: :running, plugins: actual_plugins} =
      @command.run(["fede"], Map.merge(context[:opts], %{minimal: true}))

    assert_plugin_states(actual_plugins, [%{name: :rabbitmq_federation}])

    %{status: :running, plugins: actual_plugins2} =
      @command.run(["stomp$"], Map.merge(context[:opts], %{minimal: true}))

    assert_plugin_states(actual_plugins2, [%{name: :rabbitmq_stomp}])
  end

  test "should succeed when using multiple plugins directories, one of them does not exist",
       context do
    opts = get_opts_with_non_existing_plugins_directory(context)

    expected_plugins = [
      %{name: :rabbitmq_federation},
      %{name: :rabbitmq_stomp}
    ]

    %{status: :running, plugins: actual_plugins} =
      @command.run([".*"], Map.merge(opts, %{minimal: true}))

    assert_plugin_states(actual_plugins, expected_plugins)
  end

  test "run: succeeds when using multiple plugins directories, directories do exist and do contain plugins",
       context do
    reset_enabled_plugins_to_preconfigured_defaults(context)
    opts = get_opts_with_existing_plugins_directory(context)

    expected_plugins = [%{name: :rabbitmq_federation}, %{name: :rabbitmq_stomp}]

    %{status: :running, plugins: actual_plugins} =
      @command.run([".*"], Map.merge(opts, %{minimal: true}))

    assert_plugin_states(actual_plugins, expected_plugins)
  end

  test "run: lists plugins when using multiple plugins directories", context do
    reset_enabled_plugins_to_preconfigured_defaults(context)

    plugins_directory = fixture_plugins_path("plugins-subdirectory-01")
    opts = get_opts_with_plugins_directories(context, [plugins_directory])
    switch_plugins_directories(context[:opts][:plugins_dir], opts[:plugins_dir])
    reset_enabled_plugins_to_preconfigured_defaults(context)

    expected_plugins = [
      %{name: :mock_rabbitmq_plugins_01},
      %{name: :mock_rabbitmq_plugins_02},
      %{name: :rabbitmq_federation},
      %{name: :rabbitmq_stomp}
    ]

    %{
      plugins: actual_plugins
    } = @command.run([".*"], Map.merge(opts, %{minimal: true}))

    assert_plugin_states(actual_plugins, expected_plugins)
  end

  test "run: lists the most recent version of every plugin", context do
    reset_enabled_plugins_to_preconfigured_defaults(context)

    plugins_directory_01 = fixture_plugins_path("plugins-subdirectory-01")
    plugins_directory_02 = fixture_plugins_path("plugins-subdirectory-02")

    opts =
      get_opts_with_plugins_directories(context, [plugins_directory_01, plugins_directory_02])

    switch_plugins_directories(context[:opts][:plugins_dir], opts[:plugins_dir])
    reset_enabled_plugins_to_preconfigured_defaults(context)

    expected_plugins = [
      %{
        name: :mock_rabbitmq_plugins_01,
        enabled: :not_enabled,
        running: false,
        version: ~c"0.2.0"
      },
      %{
        name: :mock_rabbitmq_plugins_02,
        enabled: :not_enabled,
        running: false,
        version: ~c"0.2.0"
      },
      %{name: :rabbitmq_federation, enabled: :enabled, running: true},
      %{name: :rabbitmq_stomp, enabled: :enabled, running: true}
    ]

    %{
      plugins: actual_plugins
    } = @command.run([".*"], opts)

    assert_plugin_states(actual_plugins, expected_plugins)
  end

  test "run: reports both running and pending upgrade versions", context do
    reset_enabled_plugins_to_preconfigured_defaults(context)

    plugins_directory_01 = fixture_plugins_path("plugins-subdirectory-01")
    plugins_directory_02 = fixture_plugins_path("plugins-subdirectory-02")
    opts = get_opts_with_plugins_directories(context, [plugins_directory_01])
    switch_plugins_directories(context[:opts][:plugins_dir], opts[:plugins_dir])

    set_enabled_plugins(
      [:mock_rabbitmq_plugins_02, :rabbitmq_federation, :rabbitmq_stomp],
      :online,
      get_rabbit_hostname(),
      opts
    )

    expected_plugins = [
      %{
        name: :mock_rabbitmq_plugins_01,
        enabled: :not_enabled,
        running: false,
        version: ~c"0.2.0"
      },
      %{
        name: :mock_rabbitmq_plugins_02,
        enabled: :enabled,
        running: true,
        version: ~c"0.1.0",
        running_version: ~c"0.1.0"
      },
      %{name: :rabbitmq_federation, enabled: :enabled, running: true},
      %{name: :rabbitmq_stomp, enabled: :enabled, running: true}
    ]

    %{
      status: :running,
      plugins: actual_plugins
    } = @command.run([".*"], opts)

    assert_plugin_states(actual_plugins, expected_plugins)

    opts =
      get_opts_with_plugins_directories(context, [plugins_directory_01, plugins_directory_02])

    switch_plugins_directories(context[:opts][:plugins_dir], opts[:plugins_dir])

    expected_plugins2 = [
      %{
        name: :mock_rabbitmq_plugins_01,
        enabled: :not_enabled,
        running: false,
        version: ~c"0.2.0"
      },
      %{
        name: :mock_rabbitmq_plugins_02,
        enabled: :enabled,
        running: true,
        version: ~c"0.2.0",
        running_version: ~c"0.1.0"
      },
      %{name: :rabbitmq_federation, enabled: :enabled, running: true},
      %{name: :rabbitmq_stomp, enabled: :enabled, running: true}
    ]

    %{
      plugins: actual_plugins2
    } = @command.run([".*"], opts)

    assert_plugin_states(actual_plugins2, expected_plugins2)
  end
end
