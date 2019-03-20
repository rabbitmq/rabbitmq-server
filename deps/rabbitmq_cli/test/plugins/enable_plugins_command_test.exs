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

defmodule EnablePluginsCommandTest do
  use ExUnit.Case, async: false
  import TestHelper

  alias RabbitMQ.CLI.Core.ExitCodes

  @command RabbitMQ.CLI.Plugins.Commands.EnableCommand

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

  test "validate: not specifying any plugins to enable is reported as invalid", context do
    assert match?(
      {:validation_failure, :not_enough_args},
      @command.validate([], Map.merge(context[:opts], %{online: true, offline: false}))
    )
  end

  test "validate_execution_environment: not specifying an enabled_plugins_file is reported as an error", context do
    assert @command.validate_execution_environment(["a"], Map.delete(context[:opts], :enabled_plugins_file)) ==
      {:validation_failure, :no_plugins_file}
  end

  test "validate_execution_environment: not specifying a plugins_dir is reported as an error", context do
    assert @command.validate_execution_environment(["a"], Map.delete(context[:opts], :plugins_dir)) ==
      {:validation_failure, :no_plugins_dir}
  end


  test "validate_execution_environment: specifying a non-existent enabled_plugins_file is fine", context do
    assert @command.validate_execution_environment(["a"], Map.merge(context[:opts], %{enabled_plugins_file: "none"})) == :ok
  end

  test "validate_execution_environment: specifying a non-existent plugins_dir is reported as an error", context do
    assert @command.validate_execution_environment(["a"], Map.merge(context[:opts], %{plugins_dir: "none"})) ==
      {:validation_failure, :plugins_dir_does_not_exist}
  end

  test "validate: failure to load the rabbit application is reported as an error", context do
    assert {:validation_failure, {:unable_to_load_rabbit, _}} =
      @command.validate_execution_environment(["a"], Map.delete(context[:opts], :rabbitmq_home))
  end

  test "if node is inaccessible, writes enabled plugins file and reports implicitly enabled plugin list", context do
    # Clears enabled plugins file
    set_enabled_plugins([], :offline, :nonode, context[:opts])

    assert {:stream, test_stream} =
      @command.run(["rabbitmq_stomp"], Map.merge(context[:opts], %{node: :nonode}))
    assert [[:rabbitmq_stomp],
            %{mode: :offline, enabled: [:rabbitmq_stomp], set: [:rabbitmq_stomp]}] ==
      Enum.to_list(test_stream)
    check_plugins_enabled([:rabbitmq_stomp], context)
    assert [:amqp_client, :rabbitmq_federation, :rabbitmq_stomp] ==
           currently_active_plugins(context)
  end

  test "in offline mode, writes enabled plugins and reports implicitly enabled plugin list", context do
    # Clears enabled plugins file
    set_enabled_plugins([], :offline, :nonode, context[:opts])

    assert {:stream, test_stream} =
      @command.run(["rabbitmq_stomp"], Map.merge(context[:opts], %{offline: true, online: false}))
    assert [[:rabbitmq_stomp],
            %{mode: :offline, enabled: [:rabbitmq_stomp], set: [:rabbitmq_stomp]}] ==
      Enum.to_list(test_stream)
    check_plugins_enabled([:rabbitmq_stomp], context)
    assert_equal_sets(
      [:amqp_client, :rabbitmq_federation, :rabbitmq_stomp],
      currently_active_plugins(context))
  end

  test "adds additional plugins to those already enabled", context do
    # Clears enabled plugins file
    set_enabled_plugins([], :offline, :nonode, context[:opts])

    assert {:stream, test_stream0} =
      @command.run(["rabbitmq_stomp"], Map.merge(context[:opts], %{offline: true, online: false}))
    assert [[:rabbitmq_stomp],
            %{mode: :offline, enabled: [:rabbitmq_stomp], set: [:rabbitmq_stomp]}] ==
      Enum.to_list(test_stream0)
    check_plugins_enabled([:rabbitmq_stomp], context)
    assert {:stream, test_stream1} =
      @command.run(["rabbitmq_federation"], Map.merge(context[:opts], %{offline: true, online: false}))
    assert [[:rabbitmq_federation, :rabbitmq_stomp],
            %{mode: :offline, enabled: [:rabbitmq_federation],
              set: [:rabbitmq_federation, :rabbitmq_stomp]}] ==
      Enum.to_list(test_stream1)
    check_plugins_enabled([:rabbitmq_stomp, :rabbitmq_federation], context)
  end

  test "updates plugin list and starts newly enabled plugins", context do
    # Clears enabled plugins file and stop all plugins
    set_enabled_plugins([], :online, context[:opts][:node], context[:opts])

    assert {:stream, test_stream0} =
      @command.run(["rabbitmq_stomp"], context[:opts])
    assert [[:rabbitmq_stomp],
            %{mode: :online,
              started: [:rabbitmq_stomp], stopped: [],
              enabled: [:rabbitmq_stomp],
              set: [:rabbitmq_stomp]}] ==
      Enum.to_list(test_stream0)

    check_plugins_enabled([:rabbitmq_stomp], context)
    assert_equal_sets([:amqp_client, :rabbitmq_stomp], currently_active_plugins(context))

    {:stream, test_stream1} =
      @command.run(["rabbitmq_federation"], context[:opts])
    assert [[:rabbitmq_federation, :rabbitmq_stomp],
            %{mode: :online,
              started: [:rabbitmq_federation], stopped: [],
              enabled: [:rabbitmq_federation],
              set: [:rabbitmq_federation, :rabbitmq_stomp]}] ==
      Enum.to_list(test_stream1)

    check_plugins_enabled([:rabbitmq_stomp, :rabbitmq_federation], context)
    assert_equal_sets([:amqp_client, :rabbitmq_federation, :rabbitmq_stomp], currently_active_plugins(context))
  end

  test "can enable multiple plugins at once", context do
    # Clears plugins file and stop all plugins
    set_enabled_plugins([], :online, context[:opts][:node], context[:opts])

    assert {:stream, test_stream} =
      @command.run(["rabbitmq_stomp", "rabbitmq_federation"], context[:opts])
    assert [[:rabbitmq_federation, :rabbitmq_stomp],
            %{mode: :online,
              started: [:rabbitmq_federation, :rabbitmq_stomp], stopped: [],
              enabled: [:rabbitmq_federation, :rabbitmq_stomp],
              set: [:rabbitmq_federation, :rabbitmq_stomp]}] ==
      Enum.to_list(test_stream)
    check_plugins_enabled([:rabbitmq_stomp, :rabbitmq_federation], context)

    assert_equal_sets([:amqp_client, :rabbitmq_federation, :rabbitmq_stomp], currently_active_plugins(context))
  end

  test "does not enable an already implicitly enabled plugin", context do
    # Clears enabled plugins file and stop all plugins
    set_enabled_plugins([:rabbitmq_federation], :online, context[:opts][:node], context[:opts])
    assert {:stream, test_stream} =
      @command.run(["amqp_client"], context[:opts])
    assert [[:rabbitmq_federation],
            %{mode: :online,
              started: [], stopped: [],
              enabled: [],
              set: [:rabbitmq_federation]}] ==
      Enum.to_list(test_stream)
    check_plugins_enabled([:rabbitmq_federation], context)
    assert [:amqp_client, :rabbitmq_federation] ==
           currently_active_plugins(context)

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
    ## Not changed
    check_plugins_enabled([:mock_rabbitmq_plugin_for_3_7], context)
  end

  test "run: does not enable plugins with unmet version requirements even when enabling all plugins", context do
    set_enabled_plugins([], :online, context[:opts][:node], context[:opts])

    plugins_directory = fixture_plugins_path("plugins_with_version_requirements")
    opts = get_opts_with_plugins_directories(context, [plugins_directory])
    opts = Map.merge(opts, %{all: true})
    switch_plugins_directories(context[:opts][:plugins_dir], opts[:plugins_dir])

    {:error, _version_error} = @command.run([], opts)

    check_plugins_enabled([], context)
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
