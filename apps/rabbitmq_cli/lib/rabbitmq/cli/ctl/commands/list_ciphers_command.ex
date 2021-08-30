## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.

defmodule RabbitMQ.CLI.Ctl.Commands.ListCiphersCommand do
  alias RabbitMQ.CLI.Core.DocGuide

  @behaviour RabbitMQ.CLI.CommandBehaviour
  use RabbitMQ.CLI.DefaultOutput

  def scopes(), do: [:ctl, :diagnostics]

  use RabbitMQ.CLI.Core.MergesNoDefaults
  use RabbitMQ.CLI.Core.AcceptsNoPositionalArguments

  def distribution(_), do: :none

  def run(_, _) do
    {:ok, :rabbit_pbe.supported_ciphers()}
  end

  def formatter(), do: RabbitMQ.CLI.Formatters.Erlang

  def usage, do: "list_ciphers"

  def usage_doc_guides() do
    [
      DocGuide.configuration(),
      DocGuide.tls()
    ]
  end

  def help_section(), do: :observability_and_health_checks

  def description(), do: "Lists cipher suites supported by encoding commands"

  def banner(_, _), do: "Listing supported ciphers ..."
end
