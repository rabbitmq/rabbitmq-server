## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.

defmodule RabbitMQ.CLI.Core.Distribution do
  alias RabbitMQ.CLI.Core.{Config, Helpers}

  #
  # API
  #

  def start() do
    start(%{})
  end

  def start(options) do
    node_name_type = Config.get_option(:longnames, options)
    result = start(node_name_type, 10, :undefined)
    ensure_cookie(options)
    result
  end

  def stop, do: Node.stop()

  def start_as(node_name, options) do
    node_name_type = Config.get_option(:longnames, options)
    result = start_with_epmd(node_name, node_name_type)
    ensure_cookie(options)
    result
  end

  ## Optimization. We try to start EPMD only if distribution fails
  def start_with_epmd(node_name, node_name_type) do
    case Node.start(node_name, node_name_type) do
      {:ok, _} = ok ->
        ok

      {:error, {:already_started, _}} = started ->
        started

      {:error, {{:already_started, _}, _}} = started ->
        started

      ## EPMD can be stopped. Retry with EPMD
      {:error, _} ->
        :rabbit_nodes_common.ensure_epmd()
        Node.start(node_name, node_name_type)
    end
  end

  def per_node_timeout(:infinity, _) do
    :infinity
  end

  def per_node_timeout(timeout, node_count) do
    Kernel.trunc(timeout / node_count)
  end

  #
  # Implementation
  #

  def ensure_cookie(options) do
    case Config.get_option(:erlang_cookie, options) do
      nil ->
        :ok

      cookie ->
        Node.set_cookie(cookie)
        :ok
    end
  end

  defp start(_opt, 0, last_err) do
    {:error, last_err}
  end

  defp start(node_name_type, attempts, _last_err) do
    candidate = generate_cli_node_name(node_name_type)

    case start_with_epmd(candidate, node_name_type) do
      {:ok, _} ->
        :ok

      {:error, {:already_started, pid}} ->
        {:ok, pid}

      {:error, {{:already_started, pid}, _}} ->
        {:ok, pid}

      {:error, reason} ->
        start(node_name_type, attempts - 1, reason)
    end
  end

  defp generate_cli_node_name(node_name_type) do
    case Helpers.get_rabbit_hostname(node_name_type) do
      {:error, _} = err ->
        throw(err)

      rmq_hostname ->
        # This limits the number of possible unique node names used by CLI tools to avoid
        # the atom table from growing above the node limit. We must use reasonably unique IDs
        # to allow for concurrent CLI tool execution.
        #
        # Enum.random/1 is constant time and space with range arguments https://hexdocs.pm/elixir/Enum.html#random/1.
        id = Enum.random(1..1024)
        String.to_atom("rabbitmqcli-#{id}-#{rmq_hostname}")
    end
  end
end
