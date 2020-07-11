## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2016-2020 VMware, Inc. or its affiliates.  All rights reserved.


defmodule UpdateClusterNodesCommandTest do
  use ExUnit.Case, async: false
  import TestHelper

  @command RabbitMQ.CLI.Ctl.Commands.UpdateClusterNodesCommand

  setup_all do
    RabbitMQ.CLI.Core.Distribution.start()

    start_rabbitmq_app()

    on_exit([], fn ->
      start_rabbitmq_app()
    end)

    :ok
  end

  setup do
    {:ok, opts: %{
      node: get_rabbit_hostname()
    }}
  end

  test "validate: providing too few arguments fails validation", context do
    assert @command.validate([], context[:opts]) ==
      {:validation_failure, :not_enough_args}
  end

  test "validate: providing too many arguments fails validation", context do
    assert @command.validate(["a", "b", "c"], context[:opts]) ==
      {:validation_failure, :too_many_args}
  end

  test "run: specifying self as seed node fails validation", context do
    stop_rabbitmq_app()
    assert match?(
      {:error, :cannot_cluster_node_with_itself},
      @command.run([context[:opts][:node]], context[:opts]))
    start_rabbitmq_app()
  end

  test "run: request to an unreachable node returns a badrpc", context do
    opts = %{
      node: :jake@thedog,
      timeout: 200
    }
    assert match?(
      {:badrpc, :nodedown},
      @command.run([context[:opts][:node]], opts))
  end

  test "run: specifying an unreachable node as seed returns a badrpc", context do
    stop_rabbitmq_app()
    assert match?(
      {:badrpc_multi, _, [_]},
      @command.run([:jake@thedog], context[:opts]))
    start_rabbitmq_app()
  end

  test "banner", context do
    assert @command.banner(["a"], context[:opts]) =~
      ~r/Will seed #{get_rabbit_hostname()} from a on next start/
  end

  test "output mnesia is running error", context do
    exit_code = RabbitMQ.CLI.Core.ExitCodes.exit_software
    assert match?({:error, ^exit_code,
                   "Mnesia is still running on node " <> _},
                   @command.output({:error, :mnesia_unexpectedly_running}, context[:opts]))

  end
end
