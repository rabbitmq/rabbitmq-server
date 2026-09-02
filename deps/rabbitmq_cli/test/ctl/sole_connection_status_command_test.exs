## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries.  All rights reserved.

defmodule SoleConnectionStatusCommandTest do
  use ExUnit.Case, async: false
  import TestHelper

  @command RabbitMQ.CLI.Ctl.Commands.SoleConnectionStatusCommand

  setup_all do
    RabbitMQ.CLI.Core.Distribution.start()

    :ok
  end

  setup context do
    {:ok, opts: %{node: get_rabbit_hostname(), timeout: context[:test_timeout] || 30_000}}
  end

  test "validate: with extra arguments returns an arg count error", context do
    assert @command.validate(["extra"], context[:opts]) == {:validation_failure, :too_many_args}
  end

  @tag test_timeout: 3000
  test "run: request to a non-existent node returns a badrpc" do
    opts = %{node: :jake@thedog, timeout: 200}
    assert match?({:badrpc, _}, @command.run([], opts))
  end

  test "run: with a registered connection returns RA status information", context do
    node = context[:opts][:node]
    pid = :rpc.call(node, :erlang, :spawn, [:timer, :sleep, [:infinity]])

    on_exit(fn -> :rpc.call(node, :erlang, :exit, [pid, :kill]) end)

    :ok =
      :rpc.call(node, :rabbit_sole_conn, :acquire, [
        :refuse_connection,
        "/",
        "rabbitmq-cli-test-sole-conn",
        "guest",
        pid
      ])

    result = @command.run([], context[:opts])

    assert is_list(result)
    assert length(result) > 0

    Enum.each(result, fn metrics ->
      assert :proplists.get_value(<<"Node Name">>, metrics) != :undefined
      assert :proplists.get_value(<<"Raft State">>, metrics) != :undefined
    end)
  end

  test "output: an unavailable sole_conn enforcement feature is reported clearly", context do
    assert {:error, _, msg} =
             @command.output({:error, :sole_conn_not_started_or_available}, context[:opts])

    assert msg =~ ~r/not started or unavailable/
  end

  test "output: a disabled feature flag is reported clearly", context do
    assert {:error, _, msg} =
             @command.output(
               {:error, {:feature_flag_disabled, :"rabbitmq_4.4.0"}},
               context[:opts]
             )

    assert msg =~ ~r/rabbitmq_4\.4\.0/
  end

  test "banner", context do
    assert @command.banner([], context[:opts]) =~
             ~r/Status of AMQP 1.0 sole connection enforcement/
  end
end
