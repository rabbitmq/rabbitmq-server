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

  @command RabbitMQ.CLI.Plugins.Commands.EnableCommand
  @vhost "test1"
  @user "guest"
  @root   "/"
  @default_timeout :infinity

  setup_all do
    RabbitMQ.CLI.Core.Distribution.start()
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
             online: true, offline: false,
             all: false}

    on_exit(fn ->
      set_enabled_plugins(enabled_plugins, :online, get_rabbit_hostname, opts)
    end)

    :erlang.disconnect_node(node)


    {:ok, opts: opts}
  end

  setup context do
    :net_kernel.connect_node(get_rabbit_hostname)
    set_enabled_plugins([:rabbitmq_stomp, :rabbitmq_federation],
                        :online,
                        get_rabbit_hostname,
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
      @command.validate(["a"], Map.merge(context[:opts], %{online: true, offline: true}))
    )
  end

  test "validate: not specifying a plugins to enable is reported as invalid", context do
    assert match?(
      {:validation_failure, :not_enough_arguments},
      @command.validate([], Map.merge(context[:opts], %{online: true, offline: false}))
    )
  end

  test "validate: not specifying an enabled_plugins_file is reported as an error", context do
    assert @command.validate(["a"], Map.delete(context[:opts], :enabled_plugins_file)) ==
      {:validation_failure, :no_plugins_file}
  end

  test "validate: not specifying a plugins_dir is reported as an error", context do
    assert @command.validate(["a"], Map.delete(context[:opts], :plugins_dir)) ==
      {:validation_failure, :no_plugins_dir}
  end


  test "validate: specifying a non-existent enabled_plugins_file is fine", context do
    assert @command.validate(["a"], Map.merge(context[:opts], %{enabled_plugins_file: "none"})) == :ok
  end

  test "validate: specifying a non-existent plugins_dir is reported as an error", context do
    assert @command.validate(["a"], Map.merge(context[:opts], %{plugins_dir: "none"})) ==
      {:validation_failure, :plugins_dir_does_not_exist}
  end

  test "validate: failure to load the rabbit application is reported as an error", context do
    assert {:validation_failure, {:unable_to_load_rabbit, _}} =
      @command.validate(["a"], Map.delete(context[:opts], :rabbitmq_home))
  end

  test "if node is unaccessible, writes enabled plugins file and reports implicitly enabled plugin list", context do
    # Clears enabled plugins file
    set_enabled_plugins([], :offline, :nonode, context[:opts])

    assert {:stream, test_stream} =
      @command.run(["rabbitmq_stomp"], Map.merge(context[:opts], %{node: :nonode}))
    assert [[:amqp_client, :rabbitmq_stomp],
            %{mode: :offline, enabled: [:amqp_client, :rabbitmq_stomp], set: [:amqp_client, :rabbitmq_stomp]}] ==
      Enum.to_list(test_stream)
    assert {:ok, [[:rabbitmq_stomp]]} == :file.consult(context[:opts][:enabled_plugins_file])
    assert [:amqp_client, :rabbitmq_federation, :rabbitmq_stomp] ==
           currently_active_plugins(context)
  end

  test "in offline mode, writes enabled plugins and reports implicitly enabled plugin list", context do
    # Clears enabled plugins file
    set_enabled_plugins([], :offline, :nonode, context[:opts])

    assert {:stream, test_stream} =
      @command.run(["rabbitmq_stomp"], Map.merge(context[:opts], %{offline: true, online: false}))
    assert [[:amqp_client, :rabbitmq_stomp],
            %{mode: :offline, enabled: [:amqp_client, :rabbitmq_stomp], set: [:amqp_client, :rabbitmq_stomp]}] ==
      Enum.to_list(test_stream)
    assert {:ok, [[:rabbitmq_stomp]]} == :file.consult(context[:opts][:enabled_plugins_file])

    assert_equal_sets(
      [:amqp_client, :rabbitmq_federation, :rabbitmq_stomp],
      currently_active_plugins(context))
  end

  test "adds additional plugins to those already enabled", context do
    # Clears enabled plugins file
    set_enabled_plugins([], :offline, :nonode, context[:opts])

    assert {:stream, test_stream0} =
      @command.run(["rabbitmq_stomp"], Map.merge(context[:opts], %{offline: true, online: false}))
    assert [[:amqp_client, :rabbitmq_stomp],
            %{mode: :offline, enabled: [:amqp_client, :rabbitmq_stomp], set: [:amqp_client, :rabbitmq_stomp]}] ==
      Enum.to_list(test_stream0)
    assert {:ok, [[:rabbitmq_stomp]]} == :file.consult(context[:opts][:enabled_plugins_file])

    assert {:stream, test_stream1} =
      @command.run(["rabbitmq_federation"], Map.merge(context[:opts], %{offline: true, online: false}))
    assert [[:amqp_client, :rabbitmq_federation, :rabbitmq_stomp],
            %{mode: :offline, enabled: [:rabbitmq_federation],
              set: [:amqp_client, :rabbitmq_federation, :rabbitmq_stomp]}] ==
      Enum.to_list(test_stream1)
    {:ok, [xs]} = :file.consult(context[:opts][:enabled_plugins_file])

    assert_equal_sets([:rabbitmq_stomp, :rabbitmq_federation], xs)
  end

  test "updates plugin list and starts newly enabled plugins", context do
    # Clears enabled plugins file and stop all plugins
    set_enabled_plugins([], :online, context[:opts][:node], context[:opts])

    assert {:stream, test_stream0} =
      @command.run(["rabbitmq_stomp"], context[:opts])
    assert [[:amqp_client, :rabbitmq_stomp],
            %{mode: :online,
              started: [:amqp_client, :rabbitmq_stomp], stopped: [],
              enabled: [:amqp_client, :rabbitmq_stomp],
              set: [:amqp_client, :rabbitmq_stomp]}] ==
      Enum.to_list(test_stream0)
    assert {:ok, [xs]} = :file.consult(context[:opts][:enabled_plugins_file])

    assert_equal_sets([:rabbitmq_stomp], xs)
    assert_equal_sets([:amqp_client, :rabbitmq_stomp], currently_active_plugins(context))

    {:stream, test_stream1} =
      @command.run(["rabbitmq_federation"], context[:opts])
    assert [[:amqp_client, :rabbitmq_federation, :rabbitmq_stomp],
            %{mode: :online,
              started: [:rabbitmq_federation], stopped: [],
              enabled: [:rabbitmq_federation],
              set: [:amqp_client, :rabbitmq_federation, :rabbitmq_stomp]}] ==
      Enum.to_list(test_stream1)
    assert {:ok, [xs]} = :file.consult(context[:opts][:enabled_plugins_file])

    assert_equal_sets([:rabbitmq_stomp, :rabbitmq_federation], xs)
    assert_equal_sets([:amqp_client, :rabbitmq_federation, :rabbitmq_stomp], currently_active_plugins(context))
  end

  test "can enable multiple plugins at once", context do
    # Clears plugins file and stop all plugins
    set_enabled_plugins([], :online, context[:opts][:node], context[:opts])

    assert {:stream, test_stream} =
      @command.run(["rabbitmq_stomp", "rabbitmq_federation"], context[:opts])
    assert [[:amqp_client, :rabbitmq_federation, :rabbitmq_stomp],
            %{mode: :online,
              started: [:amqp_client, :rabbitmq_federation, :rabbitmq_stomp], stopped: [],
              enabled: [:amqp_client, :rabbitmq_federation, :rabbitmq_stomp],
              set: [:amqp_client, :rabbitmq_federation, :rabbitmq_stomp]}] ==
      Enum.to_list(test_stream)
    assert {:ok, [xs]} = :file.consult(context[:opts][:enabled_plugins_file])

    assert_equal_sets([:rabbitmq_stomp, :rabbitmq_federation], xs)
    assert_equal_sets([:amqp_client, :rabbitmq_federation, :rabbitmq_stomp], currently_active_plugins(context))
  end

  test "does not enable an already implicitly enabled plugin", context do
    # Clears enabled plugins file and stop all plugins
    set_enabled_plugins([:rabbitmq_federation], :online, context[:opts][:node], context[:opts])
    assert {:stream, test_stream} =
      @command.run(["amqp_client"], context[:opts])
    assert [[:amqp_client, :rabbitmq_federation],
            %{mode: :online,
              started: [], stopped: [],
              enabled: [],
              set: [:amqp_client, :rabbitmq_federation]}] ==
      Enum.to_list(test_stream)
    assert {:ok, [[:rabbitmq_federation]]} == :file.consult(context[:opts][:enabled_plugins_file])
    assert [:amqp_client, :rabbitmq_federation] ==
           currently_active_plugins(context)

  end

  defp assert_equal_sets(a, b) do
    assert MapSet.equal?(MapSet.new(a), MapSet.new(b))
  end
end
