## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.

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
