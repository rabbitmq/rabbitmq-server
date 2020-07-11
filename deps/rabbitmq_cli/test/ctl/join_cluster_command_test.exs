## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2016-2020 VMware, Inc. or its affiliates.  All rights reserved.


defmodule JoinClusterCommandTest do
  use ExUnit.Case, async: false
  import TestHelper

  @command RabbitMQ.CLI.Ctl.Commands.JoinClusterCommand

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
      node: get_rabbit_hostname(),
      disc: true,
      ram: false,
    }}
  end

  test "validate: specifying both --disc and --ram is reported as invalid", context do
    assert match?(
      {:validation_failure, {:bad_argument, _}},
      @command.validate(["a"], Map.merge(context[:opts], %{disc: true, ram: true}))
    )
  end
  test "validate: specifying no target node is reported as an error", context do
    assert @command.validate([], context[:opts]) ==
      {:validation_failure, :not_enough_args}
  end
  test "validate: specifying multiple target nodes is reported as an error", context do
    assert @command.validate(["a", "b", "c"], context[:opts]) ==
      {:validation_failure, :too_many_args}
  end

  # TODO
  #test "run: successful join as a disc node", context do
  #end

  # TODO
  #test "run: successful join as a RAM node", context do
  #end

  test "run: joining self is invalid", context do
    stop_rabbitmq_app()
    assert match?(
      {:error, :cannot_cluster_node_with_itself},
      @command.run([context[:opts][:node]], context[:opts]))
    start_rabbitmq_app()
  end

  # TODO
  test "run: request to an active node fails", context do
   assert match?(
     {:error, :mnesia_unexpectedly_running},
    @command.run([context[:opts][:node]], context[:opts]))
  end

  test "run: request to a non-existent node returns a badrpc", context do
    opts = %{
      node: :jake@thedog,
      disc: true,
      ram: false,
      timeout: 200
    }
    assert match?(
      {:badrpc, _},
      @command.run([context[:opts][:node]], opts))
  end

  test "run: joining a non-existent node returns a badrpc", context do
    stop_rabbitmq_app()
    assert match?(
      {:badrpc_multi, _, [_]},
      @command.run([:jake@thedog], context[:opts]))
    start_rabbitmq_app()
  end

  test "banner", context do
    assert @command.banner(["a"], context[:opts]) =~
      ~r/Clustering node #{get_rabbit_hostname()} with a/
  end

  test "output mnesia is running error", context do
    exit_code = RabbitMQ.CLI.Core.ExitCodes.exit_software
    assert match?({:error, ^exit_code,
                   "Mnesia is still running on node " <> _},
                   @command.output({:error, :mnesia_unexpectedly_running}, context[:opts]))

  end
end
