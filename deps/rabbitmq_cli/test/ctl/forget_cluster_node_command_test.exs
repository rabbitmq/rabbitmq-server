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
## The Initial Developer of the Original Code is Pivotal Software, Inc.
## Copyright (c) 2016-2017 Pivotal Software, Inc.  All rights reserved.


defmodule ForgetClusterNodeCommandTest do
  use ExUnit.Case, async: false
  import TestHelper

  @command RabbitMQ.CLI.Ctl.Commands.ForgetClusterNodeCommand

  setup_all do
    RabbitMQ.CLI.Core.Distribution.start()
    node = get_rabbit_hostname()

    start_rabbitmq_app()
    {:ok, plugins_dir} = :rabbit_misc.rpc_call(node,
                                               :application, :get_env,
                                               [:rabbit, :plugins_dir])
    rabbitmq_home = :rabbit_misc.rpc_call(node, :code, :lib_dir, [:rabbit])
    mnesia_dir = :rabbit_misc.rpc_call(node, :rabbit_mnesia, :dir, [])
    feature_flags_file = :rabbit_misc.rpc_call(node,
      :rabbit_feature_flags, :enabled_feature_flags_list_file, [])

    on_exit([], fn ->
      start_rabbitmq_app()
    end)

    {:ok, opts: %{rabbitmq_home: rabbitmq_home,
                  plugins_dir: plugins_dir,
                  mnesia_dir: mnesia_dir,
                  feature_flags_file: feature_flags_file,
                  offline: false}}
  end

  setup context do
    {:ok, opts: Map.merge(context[:opts], %{node: get_rabbit_hostname()})
    }
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
     @command.validate_execution_environment(["other_node@localhost"],
                       Map.merge(context[:opts], %{offline: true})))
  end

  test "validate_execution_environment: offline forget without mnesia dir fails", context do
    offline_opts = Map.merge(context[:opts],
                             %{offline: true, node: :non_exist@localhost})
    opts_without_mnesia = Map.delete(offline_opts, :mnesia_dir)
    assert match?(
      {:validation_failure, :mnesia_dir_not_found},
      @command.validate_execution_environment(["other_node@localhost"], opts_without_mnesia))
    Application.put_env(:mnesia, :dir, "/tmp")
    on_exit(fn -> Application.delete_env(:mnesia, :dir) end)
    assert match?(
      :ok,
      @command.validate_execution_environment(["other_node@localhost"], opts_without_mnesia))
    Application.delete_env(:mnesia, :dir)
    System.put_env("RABBITMQ_MNESIA_DIR", "/tmp")
    on_exit(fn -> System.delete_env("RABBITMQ_MNESIA_DIR") end)
    assert match?(
      :ok,
      @command.validate_execution_environment(["other_node@localhost"], opts_without_mnesia))
    System.delete_env("RABBITMQ_MNESIA_DIR")
    assert match?(
      :ok,
      @command.validate_execution_environment(["other_node@localhost"], offline_opts))
  end

  test "validate_execution_environment: online mode does not fail is mnesia is not loaded", context do
    opts_without_mnesia = Map.delete(context[:opts], :mnesia_dir)
    assert match?(
      :ok,
      @command.validate_execution_environment(["other_node@localhost"], opts_without_mnesia))
  end

  test "run: online request to a non-existent node returns a badrpc", context do
    assert match?(
      {:badrpc, :nodedown},
      @command.run([context[:opts][:node]],
                   Map.merge(context[:opts], %{node: :non_exist@localhost})))
  end

  test "banner", context do
    assert @command.banner(["a"], context[:opts]) =~
      ~r/Removing node a from the cluster/
  end
end
