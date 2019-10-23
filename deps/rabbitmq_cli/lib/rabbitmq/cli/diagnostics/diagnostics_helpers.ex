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
## The Initial Developer of the Original Code is GoPivotal, Inc.
## Copyright (c) 2007-2019 Pivotal Software, Inc.  All rights reserved.

defmodule RabbitMQ.CLI.Diagnostics.Helpers do
  def test_connection(hostname, port, timeout) do
      case :gen_tcp.connect(hostname, port, [], timeout) do
          {:error, _} -> :gen_tcp.connect(hostname, port, [:inet6], timeout)
          r -> r
      end
  end

  def check_port_connectivity(port, node_name, timeout) do
    regex = Regex.recompile!(~r/^(.+)@/)
    hostname = Regex.replace(regex, to_string(node_name), "") |> to_charlist
    try do
      case test_connection(hostname, port, timeout) do
        {:error, _} ->
          false

        {:ok, port} ->
          :ok = :gen_tcp.close(port)
          true
      end

      # `gen_tcp:connect/4` will throw if the port is outside of its
      # expected domain
    catch
      :exit, _ -> false
    end
  end

  def check_listener_connectivity(%{port: port}, node_name, timeout) do
    check_port_connectivity(port, node_name, timeout)
  end
end
