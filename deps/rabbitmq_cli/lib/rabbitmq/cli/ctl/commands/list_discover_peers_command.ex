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


defmodule RabbitMQ.CLI.Ctl.Commands.ListDiscoverPeersCommand do
  @behaviour RabbitMQ.CLI.CommandBehaviour

  def scopes(), do: [:ctl, :diagnostics]

  def merge_defaults(args, opts), do: {args, opts}

  def validate([_|_], _) do
    {:validation_failure, :too_many_args}
  end
  def validate(_, _), do: :ok

  def run([], %{node: node_name, timeout: timeout}) do
    nodes = :rabbit_misc.rpc_call(node_name, :rabbit_peer_discovery, :discover_cluster_nodes, [], timeout)
    backend = :rabbit_misc.rpc_call(node_name, :rabbit_peer_discovery, :backend, [], timeout)
    case nodes do
      {:ok, {[], _} } -> {backend, []}
      {:ok, {list_nodes, _} } -> {backend, list_nodes}
    end
  end

  def backend_type_banner(backend, options) do
    Enum.join(["Backend type",  RabbitMQ.CLI.Formatters.ErlangString.format_output(backend, options)], ": ")
  end

  def output({backend, []}, options) do
      {:ok,  Enum.join([backend_type_banner(backend, options),
             "No peers discovered"],  "\n" )}
  end
  def output({backend, list_nodes}, options) do
    {:ok, Enum.join([backend_type_banner(backend, options),
          RabbitMQ.CLI.Formatters.Erlang.format_output(list_nodes, options)],  "\n" )}
  end
  use RabbitMQ.CLI.DefaultOutput


  def usage, do: "list_discover_peers"

  def banner(_,_), do: "Listing discover peer nodes ..."


end