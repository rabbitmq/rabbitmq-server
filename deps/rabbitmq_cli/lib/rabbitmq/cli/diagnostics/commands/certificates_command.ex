## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.

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

  def help_section(), do: :configuration

  def description(), do: "Displays certificates (public keys) for every listener on target node that is configured to use TLS"

  def banner(_, %{node: node_name}), do: "Certificates of node #{node_name} ..."
end
