## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.

defmodule RabbitMQ.CLI.Diagnostics.Commands.CommandLineArgumentsCommand do
  alias RabbitMQ.CLI.Core.DocGuide
  @behaviour RabbitMQ.CLI.CommandBehaviour

  def scopes(), do: [:diagnostics]

  use RabbitMQ.CLI.Core.MergesNoDefaults

  def validate(_, %{formatter: "json"}) do
    {:validation_failure, :unsupported_formatter}
  end
  use RabbitMQ.CLI.Core.AcceptsNoPositionalArguments

  def run([], %{node: node_name}) do
    :rabbit_misc.rpc_call(node_name, :init, :get_arguments, [])
  end
  use RabbitMQ.CLI.DefaultOutput

  def formatter(), do: RabbitMQ.CLI.Formatters.Erlang

  def usage, do: "command_line_arguments"

  def usage_doc_guides() do
    [
      DocGuide.configuration(),
      DocGuide.monitoring()
    ]
  end

  def help_section(), do: :configuration

  def description(), do: "Displays target node's command-line arguments and flags as reported by the runtime"

  def banner(_, %{node: node_name}), do: "Command line arguments of node #{node_name} ..."
end
