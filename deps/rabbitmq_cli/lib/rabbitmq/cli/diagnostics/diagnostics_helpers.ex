## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2023 VMware, Inc. or its affiliates.  All rights reserved.

defmodule RabbitMQ.CLI.Diagnostics.Helpers do
  def test_connection(hostname_or_ip, port, timeout) do
    hostname_as_list = :rabbit_data_coercion.to_list(hostname_or_ip)

    case :gen_tcp.connect(hostname_as_list, port, [], timeout) do
      {:error, _} -> :gen_tcp.connect(hostname_as_list, port, [:inet6], timeout)
      r -> r
    end
  end

  def check_port_connectivity(port, node_name, timeout) do
    check_port_connectivity(port, node_name, nil, timeout)
  end

  def check_port_connectivity(port, node_name, nil, timeout) do
    regex = Regex.recompile!(~r/^(.+)@/)
    hostname = Regex.replace(regex, to_string(node_name), "") |> to_charlist

    check_port_connectivity(port, node_name, hostname, timeout)
  end

  def check_port_connectivity(port, _node_name, hostname_or_ip, timeout) do
    try do
      IO.puts("Will connect to #{hostname_or_ip}:#{port}")

      case test_connection(hostname_or_ip, port, timeout) do
        {:error, err} ->
          IO.puts("Error connecting to #{hostname_or_ip}:#{port}: #{err}")
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

  def check_listener_connectivity(%{port: port}, node_name, target_ip, timeout) do
    check_port_connectivity(port, node_name, target_ip, timeout)
  end
end
