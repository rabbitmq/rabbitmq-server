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
## Copyright (c) 2007-2020 Pivotal Software, Inc.  All rights reserved.

defmodule RabbitMQ.CLI.Diagnostics.Commands.CertificatesCommand do
  alias RabbitMQ.CLI.Core.DocGuide
  @behaviour RabbitMQ.CLI.CommandBehaviour

  import RabbitMQ.CLI.Core.Listeners

  use RabbitMQ.CLI.Core.MergesNoDefaults
  use RabbitMQ.CLI.Core.AcceptsNoPositionalArguments

  def run([], %{node: node_name, timeout: timeout}) do
    case :rabbit_misc.rpc_call(node_name, :rabbit_networking, :active_listeners, [], timeout) do
      {:error, _} = err ->
        err

      {:error, _, _} = err ->
        err

      xs when is_list(xs) ->
        listeners = listeners_with_certificates(listeners_on(xs, node_name))

        case listeners do
          [] -> %{}
          _ -> Enum.map(listeners, &listener_certs/1)
        end

      other ->
        other
    end
  end

  use RabbitMQ.CLI.DefaultOutput

  def formatter(), do: RabbitMQ.CLI.Formatters.Erlang

  def usage, do: "certificates"

  def usage_doc_guides() do
    [
      DocGuide.configuration(),
      DocGuide.tls()
    ]
  end

  def help_section(), do: :observability_and_health_checks

  def description(), do: "Displays certificates (public keeys) for every listener on target node that is configured to use TLS"

  def banner(_, %{node: node_name}), do: "Certificates of node #{node_name} ..."
end
