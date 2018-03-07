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


defmodule RabbitMQ.CLI.Diagnostics.Commands.DiscoverPeersCommand do
  @behaviour RabbitMQ.CLI.CommandBehaviour

  def switches(), do: [timeout: :integer]
  def aliases(), do: [t: :timeout]

  def merge_defaults(args, opts), do: {args, opts}

  def validate([_|_], _) do
    {:validation_failure, :too_many_args}
  end
  def validate(_, _), do: :ok

  def run([], %{node: node_name, timeout: timeout}) do
    :rabbit_misc.rpc_call(node_name, :rabbit_peer_discovery, :discover_cluster_nodes, [], timeout)
  end

  def output({:ok, {[], _}}, _options) do
    # This is a trick to print a string without formatting it first.
    # Insert your favorite "error, operation completed successfully" joke here.
    #
    # TODO: use a new or command-specific formatter instead?
    {:error, RabbitMQ.CLI.Core.ExitCodes.exit_ok(), "No peers discovered"}
  end
  def output({:ok, {nodes, _}}, _options) do
    {:ok, nodes}
  end
  use RabbitMQ.CLI.DefaultOutput

  def formatter(), do: RabbitMQ.CLI.Formatters.Erlang

  def usage, do: "discover_peers"

  def banner(_,_), do: "Discovering peers nodes ..."


end
