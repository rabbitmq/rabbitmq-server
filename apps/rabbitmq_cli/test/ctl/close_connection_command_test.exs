## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2020 VMware, Inc. or its affiliates.  All rights reserved.


defmodule CloseConnectionCommandTest do
  use ExUnit.Case, async: false
  import TestHelper

  alias RabbitMQ.CLI.Ctl.RpcStream

  @helpers RabbitMQ.CLI.Core.Helpers

  @command RabbitMQ.CLI.Ctl.Commands.CloseConnectionCommand

  setup_all do
    RabbitMQ.CLI.Core.Distribution.start()

    close_all_connections(get_rabbit_hostname())

    on_exit([], fn ->
      close_all_connections(get_rabbit_hostname())
    end)

    :ok
  end

  setup do
    {:ok, opts: %{node: get_rabbit_hostname(), timeout: :infinity}}
  end

  test "validate: with an invalid number of arguments returns an arg count error", context do
    assert @command.validate(["pid", "explanation", "extra"], context[:opts]) == {:validation_failure, :too_many_args}
    assert @command.validate(["pid"], context[:opts]) == {:validation_failure, :not_enough_args}
  end

  test "validate: with the correct number of arguments returns ok", context do
    assert @command.validate(["pid", "test"], context[:opts]) == :ok
  end

  test "run: a close connection request on an existing connection", context do
    with_connection("/", fn(_) ->
      Process.sleep(500)
      node = @helpers.normalise_node(context[:node], :shortnames)
      nodes = @helpers.nodes_in_cluster(node)
      [[pid: pid]] = fetch_connection_pids(node, nodes)
      assert :ok == @command.run([:rabbit_misc.pid_to_string(pid), "test"], %{node: node})
      Process.sleep(500)
      assert fetch_connection_pids(node, nodes) == []
    end)
  end

  test "run: a close connection request on for a non existing connection returns successfully", context do
    assert match?(:ok,
      @command.run(["<#{node()}.2.121.12>", "test"], %{node: @helpers.normalise_node(context[:node], :shortnames)}))
  end

  test "run: a close_connection request on nonexistent RabbitMQ node returns a badrpc" do
    opts = %{node: :jake@thedog, timeout: 200}
    assert match?({:badrpc, _}, @command.run(["<rabbit@localhost.1.2.1>", "test"], opts))
  end

  test "banner", context do
    s = @command.banner(["<rabbit@bananas.1.2.3>", "some reason"], context[:opts])
    assert s =~ ~r/Closing connection/
    assert s =~ ~r/<rabbit@bananas.1.2.3>/
  end

  defp fetch_connection_pids(node, nodes) do
    fetch_connection_pids(node, nodes, 10)
  end

  defp fetch_connection_pids(node, nodes, retries) do
      stream = RpcStream.receive_list_items(node,
                                            :rabbit_networking,
                                            :emit_connection_info_all,
                                            [nodes, [:pid]],
                                            :infinity,
                                            [:pid],
                                            Kernel.length(nodes))
      xs = Enum.to_list(stream)

      case {xs, retries} do
        {xs, 0} ->
          xs
        {[], n} when n >= 0 ->
          Process.sleep(100)
          fetch_connection_pids(node, nodes, retries - 1)
        _ ->
          xs
      end
  end

end
