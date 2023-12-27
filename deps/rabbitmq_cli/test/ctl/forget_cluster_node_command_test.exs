## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2016-2020 VMware, Inc. or its affiliates.  All rights reserved.

defmodule ForgetClusterNodeCommandTest do
  use ExUnit.Case, async: false
  import TestHelper

  @command RabbitMQ.CLI.Ctl.Commands.ForgetClusterNodeCommand

  setup_all do
    RabbitMQ.CLI.Core.Distribution.start()
    node = get_rabbit_hostname()

    start_rabbitmq_app()

    {:ok, plugins_dir} =
      :rabbit_misc.rpc_call(node, :application, :get_env, [:rabbit, :plugins_dir])

    rabbitmq_home = :rabbit_misc.rpc_call(node, :code, :lib_dir, [:rabbit])
    data_dir = :rabbit_misc.rpc_call(node, :rabbit, :data_dir, [])

    feature_flags_file =
      :rabbit_misc.rpc_call(node, :rabbit_feature_flags, :enabled_feature_flags_list_file, [])

    on_exit([], fn ->
      start_rabbitmq_app()
    end)

    {:ok,
     opts: %{
       rabbitmq_home: rabbitmq_home,
       plugins_dir: plugins_dir,
       data_dir: data_dir,
       feature_flags_file: feature_flags_file,
       offline: false
     }}
  end

  setup context do
    {:ok, opts: Map.merge(context[:opts], %{node: get_rabbit_hostname()})}
  end

  test "validate: specifying no target node is reported as an error", context do
    assert @command.validate([], context[:opts]) ==
             {:validation_failure, :not_enough_args}
  end

  test "validate: specifying multiple target nodes is reported as an error", context do
    assert @command.validate(["a", "b", "c"], context[:opts]) ==
             {:validation_failure, :too_many_args}
  end

  test "validate_execution_environment: offline request to a running node fails", context do
    assert match?(
             {:validation_failure, :node_running},
             @command.validate_execution_environment(
               ["other_node@localhost"],
               Map.merge(context[:opts], %{offline: true})
             )
           )
  end

  test "validate_execution_environment: offline forget without data dir fails", context do
    offline_opts =
      Map.merge(
        context[:opts],
        %{offline: true, node: :non_exist@localhost}
      )

    opts_without_data_dir = Map.delete(offline_opts, :data_dir)
    Application.put_env(:rabbit, :data_dir, "/tmp")
    on_exit(fn -> Application.delete_env(:rabbit, :data_dir) end)

    assert match?(
             :ok,
             @command.validate_execution_environment(
               ["other_node@localhost"],
               opts_without_data_dir
             )
           )

    Application.delete_env(:rabbit, :data_dir)
    System.put_env("RABBITMQ_MNESIA_DIR", "/tmp")
    on_exit(fn -> System.delete_env("RABBITMQ_MNESIA_DIR") end)

    assert match?(
             :ok,
             @command.validate_execution_environment(
               ["other_node@localhost"],
               opts_without_data_dir
             )
           )

    System.delete_env("RABBITMQ_MNESIA_DIR")

    assert match?(
             :ok,
             @command.validate_execution_environment(["other_node@localhost"], offline_opts)
           )
  end

  test "validate_execution_environment: online mode does not fail if database is not loaded",
       context do
    opts_without_data_dir = Map.delete(context[:opts], :data_dir)

    assert match?(
             :ok,
             @command.validate_execution_environment(
               ["other_node@localhost"],
               opts_without_data_dir
             )
           )
  end

  test "run: online request to a non-existent node returns a badrpc", context do
    assert match?(
             {:badrpc, :nodedown},
             @command.run(
               [context[:opts][:node]],
               Map.merge(context[:opts], %{node: :non_exist@localhost})
             )
           )
  end

  test "banner", context do
    assert @command.banner(["a"], context[:opts]) =~
             ~r/Removing node a from the cluster/
  end
end
