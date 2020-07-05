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
## Copyright (c) 2007-2020 VMware, Inc. or its affiliates.  All rights reserved.

defmodule RabbitMQ.CLI.Diagnostics.Commands.ResolverInfoCommand do
  @moduledoc """
  Displays effective hostname resolver (inetrc) configuration on target node
  """

  import RabbitMQ.CLI.Core.Platform, only: [line_separator: 0]
  import RabbitMQ.CLI.Core.ANSI, only: [bright: 1]
  alias RabbitMQ.CLI.Core.Networking

  @behaviour RabbitMQ.CLI.CommandBehaviour

  def scopes(), do: [:diagnostics]

  def switches(), do: [offline: :boolean]
  def aliases(), do: []

  def merge_defaults(args, opts) do
    {args, Map.merge(%{offline: false}, opts)}
  end

  def validate(args, _) when length(args) > 0, do: {:validation_failure, :too_many_args}
  def validate([], _), do: :ok

  def run([], %{offline: true}) do
    Networking.inetrc_map(:inet.get_rc())
  end
  def run([], %{node: node_name, timeout: timeout, offline: false}) do
    case :rabbit_misc.rpc_call(node_name, :inet, :get_rc, [], timeout) do
      {:error, _} = err -> err
      {:error, _, _} = err -> err
      xs when is_list(xs) -> Networking.inetrc_map(xs)
      other -> other
    end
  end

  def output(info, %{node: node_name, formatter: "json"}) do
    {:ok, %{
      "result"   => "ok",
      "node"     => node_name,
      "resolver" => info
    }}
  end
  def output(info, _opts) do
    main_section = [
      "#{bright("Runtime Hostname Resolver (inetrc) Settings")}\n",
      "Lookup order: #{info["lookup"]}",
      "Hosts file: #{info["hosts_file"]}",
      "Resolver conf file: #{info["resolv_conf"]}",
      "Cache size: #{info["cache_size"]}"
    ]
    hosts_section = [
      "\n#{bright("inetrc File Host Entries")}\n"
    ] ++ case info["hosts"] do
      []  -> ["(none)"]
      nil -> ["(none)"]
      hs  -> Enum.reduce(hs, [], fn {k, v}, acc -> ["#{k} #{Enum.join(v, ", ")}" | acc] end)
    end

    lines = main_section ++ hosts_section

    {:ok, Enum.join(lines, line_separator())}
  end

  def usage() do
    "resolver_info"
  end

  def help_section(), do: :configuration

  def description(), do: "Displays effective hostname resolver (inetrc) configuration on target node"

  def banner(_, %{node: node_name, offline: false}) do
    "Asking node #{node_name} for its effective hostname resolver (inetrc) configuration..."
  end
  def banner(_, %{offline: true}) do
    "Displaying effective hostname resolver (inetrc) configuration used by CLI tools..."
  end
end
