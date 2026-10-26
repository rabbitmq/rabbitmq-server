## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.

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
    {:ok,
     opts: %{
       node: get_rabbit_hostname(),
       disc: true
     }}
  end

  test "validate: specifying no target node is reported as an error", context do
    assert @command.validate([], context[:opts]) ==
             {:validation_failure, :not_enough_args}
  end

  test "validate: specifying multiple target nodes is reported as an error", context do
    assert @command.validate(["a", "b", "c"], context[:opts]) ==
             {:validation_failure, :too_many_args}
  end

  test "validate_execution_environment: succeeds when the rabbit app is running", context do
    opts = Map.put(context[:opts], :timeout, 10_000)
    assert :ok == @command.validate_execution_environment(["other"], opts)
  end

  test "validate_execution_environment: succeeds when the rabbit app is stopped", context do
    opts = Map.put(context[:opts], :timeout, 10_000)
    stop_rabbitmq_app()

    try do
      assert :ok == @command.validate_execution_environment(["other"], opts)
    after
      start_rabbitmq_app()
    end
  end

  test "validate_execution_environment: fails while the node is booting", context do
    opts = Map.put(context[:opts], :timeout, 10_000)
    node = get_rabbit_hostname()
    # `rabbit_boot_state:set/1` needs a supervisor that stops with the app,
    # so the key `rabbit_boot_state:get/0` reads is set directly.
    key = {:rabbit_boot_state, :boot_state}
    stop_rabbitmq_app()

    try do
      :ok = :rabbit_misc.rpc_call(node, :persistent_term, :put, [key, :booting])

      assert {:validation_failure, {:rabbit_app_is_booting_or_stopping, {node, :booting}}} ==
               @command.validate_execution_environment(["other"], opts)
    after
      :rabbit_misc.rpc_call(node, :persistent_term, :erase, [key])
      start_rabbitmq_app()
    end
  end

  # TODO
  # test "run: successful join as a disc node", context do
  # end

  test "run: joining self is invalid", context do
    stop_rabbitmq_app()

    assert match?(
             {:error, :cannot_cluster_node_with_itself},
             @command.run([context[:opts][:node]], context[:opts])
           )

    start_rabbitmq_app()
  end

  test "run: request to a non-existent node returns a badrpc", context do
    opts = %{
      node: :jake@thedog,
      disc: true,
      timeout: 200
    }

    assert match?(
             {:badrpc, _},
             @command.run([context[:opts][:node]], opts)
           )
  end

  test "run: joining a non-existent node returns a badrpc", context do
    stop_rabbitmq_app()

    assert match?(
             {:error, {:aborted_feature_flags_compat_check, {:error, {:erpc, :noconnection}}}},
             @command.run([:jake@thedog], context[:opts])
           )

    start_rabbitmq_app()
  end

  test "banner", context do
    assert @command.banner(["a"], context[:opts]) =~
             ~r/Clustering node #{get_rabbit_hostname()} with a/
  end

end
