## The contents of this file are subject to the Mozilla Public License
## Version 1.1 (the "License"); you may not use this file except in
## compliance with the License. You may obtain a copy of the License
## at http://www.mozilla.org/MPL/
##
## Software distributed under the License is distributed on an "AS IS"
## basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
## the License for the specific language governing rights and
## limitations under the License.
##
## The Original Code is RabbitMQ.
##
## The Initial Developer of the Original Code is GoPivotal, Inc.
## Copyright (c) 2007-2017 Pivotal Software, Inc.  All rights reserved.


defmodule CloseConnectionCommandTest do
  use ExUnit.Case, async: false
  import TestHelper

  alias RabbitMQ.CLI.Ctl.RpcStream, as: RpcStream

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
      node = @helpers.parse_node(context[:node])
      nodes = @helpers.nodes_in_cluster(node)
      [[pid: pid]] = fetch_connections_pids(node, nodes)
      assert :ok == @command.run([:rabbit_misc.pid_to_string(pid), "test"], %{node: node})
      assert fetch_connections_pids(node, nodes) == []
    end)
  end

  test "run: a close connection request on for a non existing connection returns error", context do
    assert match?({:error, {:not_a_connection_pid, _}},
      @command.run(["<#{node()}.2.121.12>", "test"], %{node: @helpers.parse_node(context[:node])}))
  end

  test "run: a close_connection request on nonexistent RabbitMQ node returns nodedown" do
    target = :jake@thedog

    opts = %{node: target}
    assert match?({:badrpc, :nodedown}, @command.run(["<rabbit@localhost.1.2.1>", "test"], opts))
  end

  test "banner", context do
    s = @command.banner(["<rabbit@bananas.1.2.3>", "some reason"], context[:opts])
    assert s =~ ~r/Closing connection/
    assert s =~ ~r/<rabbit@bananas.1.2.3>/
  end

  defp fetch_connections_pids(node, nodes) do
      RpcStream.receive_list_items(node,
                                   :rabbit_networking,
                                   :emit_connection_info_all,
                                   [nodes, [:pid]],
                                   :infinity,
                                   [:pid],
                                   Kernel.length(nodes))
      |> Enum.to_list
  end

end
