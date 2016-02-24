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
## Copyright (c) 2007-2016 Pivotal Software, Inc.  All rights reserved.


defmodule StatusCommandTest do
  use ExUnit.Case, async: false
  import TestHelper
  import ExUnit.CaptureIO

  setup_all do
    :net_kernel.start([:rabbitmqctl, :shortnames])
    on_exit([], fn -> :net_kernel.stop() end)
    :ok
  end

# -------------------------------------- RPC TESTS ------------------------------------
#
  setup rpc_context do
    :net_kernel.connect_node(rpc_context[:target])
    on_exit(rpc_context, fn -> :erlang.disconnect_node(rpc_context[:target]) end)
    :ok
  end

  @tag target: get_rabbit_hostname()
  test "status request on default RabbitMQ node",rpc_context do
    assert StatusCommand.status([])[:pid] != nil
  end

  @tag target: get_rabbit_hostname()
  test "status request on active RabbitMQ node", rpc_context do
    assert StatusCommand.status([node: rpc_context[:target]])[:pid] != nil
  end

  @tag target: :jake@thedog
  test "status request on nonexistent RabbitMQ node", rpc_context do
    assert StatusCommand.status([node: rpc_context[:target]]) == {:badrpc, :nodedown}
  end


# ------------------------------------ PRINT TESTS ------------------------------------
#
  setup print_context do
    target = get_rabbit_hostname
    :net_kernel.connect_node(target)
    on_exit(print_context, fn -> :erlang.disconnect_node(target) end)
    {:ok, result: StatusCommand.status([])}
  end

  test "status shows PID", print_context do
    assert capture_io(fn -> print(print_context[:result])  end) =~ ~r/PID\: \d+/
  end

  test "status shows Erlang version", print_context do
    assert capture_io(fn -> print(print_context[:result])  end) =~ ~r/OTP version: \d+/
    assert capture_io(fn -> print(print_context[:result])  end) =~ ~r/Erlang RTS version: \d+\.\d+\.\d+/
  end

  test "status shows info about the operating system", print_context do
    os_name = :os.type |> elem(1) |> Atom.to_string |> Mix.Utils.camelize
    os_regex = ~r/OS: #{os_name}\n/
    assert Regex.match?(os_regex, capture_io(fn -> print(print_context[:result]) end))
  end

  test "status shows running apps", print_context do
    assert capture_io(fn -> print(print_context[:result]) end) =~ ~r/Applications currently running\:\n/
    assert capture_io(fn -> print(print_context[:result]) end) =~ ~r/---------------------------------------\n/
    assert capture_io(fn -> print(print_context[:result]) end) =~ ~r/\[rabbit\]\s*| RabbitMQ\s*| \d+.\d+.\d+\n/
  end

  defp print(result) do
    StatusCommand.print_status(result)
  end
end
