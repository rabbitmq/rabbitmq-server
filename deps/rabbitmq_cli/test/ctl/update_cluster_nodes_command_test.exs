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
