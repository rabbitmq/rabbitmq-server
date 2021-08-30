## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.

defmodule RabbitMQ.CLI.Ctl.Commands.ListVhostLimitsCommand do
  alias RabbitMQ.CLI.Core.DocGuide

  @behaviour RabbitMQ.CLI.CommandBehaviour

  def scopes(), do: [:ctl, :diagnostics]
  def switches(), do: [global: :boolean]

  def merge_defaults(args, %{global: true} = opts) do
    {args, Map.merge(%{table_headers: true}, opts)}
  end

  def merge_defaults(args, opts) do
    {args, Map.merge(%{vhost: "/", table_headers: true}, opts)}
  end

  use RabbitMQ.CLI.Core.AcceptsNoPositionalArguments
  use RabbitMQ.CLI.Core.RequiresRabbitAppRunning

  def run([], %{node: node_name, global: true}) do
    case :rabbit_misc.rpc_call(node_name, :rabbit_vhost_limit, :list, []) do
      [] ->
        []

      {:error, err} ->
        {:error, err}

      {:badrpc, node} ->
        {:badrpc, node}

      val ->
        Enum.map(val, fn {vhost, val} ->
          {:ok, val_encoded} = JSON.encode(Map.new(val))
          [vhost: vhost, limits: val_encoded]
        end)
    end
  end

  def run([], %{node: node_name, vhost: vhost}) do
    case :rabbit_misc.rpc_call(node_name, :rabbit_vhost_limit, :list, [vhost]) do
      [] ->
        []

      {:error, err} ->
        {:error, err}

      {:badrpc, node} ->
        {:badrpc, node}

      val when is_list(val) or is_map(val) ->
        JSON.encode(Map.new(val))
    end
  end

  use RabbitMQ.CLI.DefaultOutput

  def formatter(), do: RabbitMQ.CLI.Formatters.Table

  def usage, do: "list_vhost_limits [--vhost <vhost>] [--global] [--no-table-headers]"

  def usage_additional() do
    [
      ["--global", "list global limits (those not associated with a virtual host)"]
    ]
  end

  def usage_doc_guides() do
    [
      DocGuide.virtual_hosts()
    ]
  end

  def help_section(), do: :virtual_hosts

  def description(), do: "Displays configured virtual host limits"

  def banner([], %{global: true}) do
    "Listing limits for all vhosts ..."
  end

  def banner([], %{vhost: vhost}) do
    "Listing limits for vhost \"#{vhost}\" ..."
  end
end
