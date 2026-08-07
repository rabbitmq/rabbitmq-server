## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries.  All rights reserved.

defmodule AmqpSoleConnForceDeleteCommandTest do
  use ExUnit.Case, async: false
  import TestHelper

  @command RabbitMQ.CLI.Ctl.Commands.AmqpSoleConnForceDeleteCommand
  @container_id "rabbitmq-cli-test-sole-conn-force-delete"

  setup_all do
    RabbitMQ.CLI.Core.Distribution.start()

    :ok
  end

  setup context do
    {:ok,
     opts: %{
       node: get_rabbit_hostname(),
       vhost: "/",
       timeout: context[:test_timeout] || 30_000
     }}
  end

  test "merge_defaults: adds default vhost if missing" do
    assert @command.merge_defaults([], %{}) == {[], %{vhost: "/"}}
  end

  test "merge_defaults: does not change defined vhost" do
    assert @command.merge_defaults([], %{vhost: "test_vhost"}) == {[], %{vhost: "test_vhost"}}
  end

  test "validate: providing too few arguments fails validation" do
    assert @command.validate([], %{}) == {:validation_failure, :not_enough_args}
  end

  test "validate: providing too many arguments fails validation" do
    assert @command.validate(["a", "b"], %{}) == {:validation_failure, :too_many_args}
  end

  test "validate: providing one argument passes validation" do
    assert @command.validate([@container_id], %{}) == :ok
  end

  @tag test_timeout: 3000
  test "run: request to a non-existent node returns a badrpc" do
    opts = %{node: :jake@thedog, vhost: "/", timeout: 200}
    assert match?({:badrpc, _}, @command.run([@container_id], opts))
  end

  test "run: force-deleting a lease that does not exist returns an error", context do
    node = context[:opts][:node]
    :ok = :rpc.call(node, :rabbit_amqp_sole_conn, :ensure_running, [])

    assert @command.run([@container_id], context[:opts]) == {:error, :not_found}
  end

  test "run: force-deleting an existing lease removes it", context do
    node = context[:opts][:node]
    pid = :rpc.call(node, :erlang, :spawn, [:timer, :sleep, [:infinity]])

    on_exit(fn -> :rpc.call(node, :erlang, :exit, [pid, :kill]) end)

    :ok =
      :rpc.call(node, :rabbit_amqp_sole_conn, :acquire, [
        :refuse_connection,
        "/",
        @container_id,
        "guest",
        pid
      ])

    assert @command.run([@container_id], context[:opts]) == :ok
    assert @command.run([@container_id], context[:opts]) == {:error, :not_found}
  end

  test "output: a missing lease is reported as a usage error", context do
    assert {:error, _, msg} = @command.output({:error, :not_found}, context[:opts])
    assert msg =~ ~r/Could not find/
  end

  test "output: an unavailable sole_conn enforcement feature is reported clearly", context do
    assert {:error, _, msg} =
             @command.output({:error, :sole_conn_not_started_or_available}, context[:opts])

    assert msg =~ ~r/not started or unavailable/
  end

  test "output: a disabled feature flag is reported clearly", context do
    assert {:error, _, msg} =
             @command.output({:error, :sole_conn_feature_flag_not_enabled}, context[:opts])

    assert msg =~ ~r/all nodes in the cluster/
  end

  test "banner", context do
    assert @command.banner([@container_id], context[:opts]) =~
             ~r/Force-deleting AMQP 1.0 sole connection lease for container ID "#{@container_id}"/
  end
end
