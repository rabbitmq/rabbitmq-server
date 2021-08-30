## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2016-2020 VMware, Inc. or its affiliates.  All rights reserved.

defmodule RenameClusterNodeCommandTest do
  use ExUnit.Case, async: false
  import TestHelper

  @command RabbitMQ.CLI.Ctl.Commands.RenameClusterNodeCommand

  setup_all do
    RabbitMQ.CLI.Core.Distribution.start()
    node = get_rabbit_hostname()

    start_rabbitmq_app()

    {:ok, plugins_dir} =
      :rabbit_misc.rpc_call(node, :application, :get_env, [:rabbit, :plugins_dir])

    rabbitmq_home = :rabbit_misc.rpc_call(node, :code, :lib_dir, [:rabbit])
    mnesia_dir = :rabbit_misc.rpc_call(node, :rabbit_mnesia, :dir, [])

    on_exit([], fn ->
      start_rabbitmq_app()
    end)

    {:ok, opts: %{rabbitmq_home: rabbitmq_home, plugins_dir: plugins_dir, mnesia_dir: mnesia_dir}}
  end

  setup context do
    {:ok,
     opts:
       Map.merge(
         context[:opts],
         %{node: :not_running@localhost}
       )}
  end

  test "validate: specifying no nodes fails validation", context do
    assert @command.validate([], context[:opts]) ==
             {:validation_failure, :not_enough_args}
  end

  test "validate: specifying one node only fails validation", context do
    assert @command.validate(["a"], context[:opts]) ==
             {:validation_failure, :not_enough_args}
  end

  test "validate_execution_environment: specifying an uneven number of arguments fails validation",
       context do
    assert match?(
             {:validation_failure, {:bad_argument, _}},
             @command.validate_execution_environment(["a", "b", "c"], context[:opts])
           )
  end

  test "validate_execution_environment: request to a running node fails", _context do
    node = get_rabbit_hostname()

    assert match?(
             {:validation_failure, :node_running},
             @command.validate_execution_environment([to_string(node), "other_node@localhost"], %{
               node: node
             })
           )
  end

  test "validate_execution_environment: not providing node mnesia dir fails validation",
       context do
    opts_without_mnesia = Map.delete(context[:opts], :mnesia_dir)
    Application.put_env(:mnesia, :dir, "/tmp")
    on_exit(fn -> Application.delete_env(:mnesia, :dir) end)

    assert :ok ==
             @command.validate(
               ["some_node@localhost", "other_node@localhost"],
               opts_without_mnesia
             )

    Application.delete_env(:mnesia, :dir)
    System.put_env("RABBITMQ_MNESIA_DIR", "/tmp")
    on_exit(fn -> System.delete_env("RABBITMQ_MNESIA_DIR") end)

    assert :ok ==
             @command.validate(
               ["some_node@localhost", "other_node@localhost"],
               opts_without_mnesia
             )

    System.delete_env("RABBITMQ_MNESIA_DIR")

    assert :ok ==
             @command.validate(["some_node@localhost", "other_node@localhost"], context[:opts])
  end

  test "banner", context do
    assert @command.banner(["a", "b"], context[:opts]) =~
             ~r/Renaming cluster nodes: \n a -> b/
  end
end
