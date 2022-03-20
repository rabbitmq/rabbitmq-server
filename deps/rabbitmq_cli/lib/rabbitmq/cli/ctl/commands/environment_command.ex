## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.

defmodule RabbitMQ.CLI.Ctl.Commands.EnvironmentCommand do
  alias RabbitMQ.CLI.Core.DocGuide
  @behaviour RabbitMQ.CLI.CommandBehaviour

  def scopes(), do: [:ctl, :diagnostics]

  use RabbitMQ.CLI.Core.MergesNoDefaults
  use RabbitMQ.CLI.Core.AcceptsNoPositionalArguments

  def run([], %{node: node_name}) do
    :rabbit_misc.rpc_call(node_name, :rabbit, :environment, [])
  end

  use RabbitMQ.CLI.DefaultOutput

  def formatter(), do: RabbitMQ.CLI.Formatters.Erlang

  def usage, do: "environment"

  def usage_doc_guides() do
    [
      DocGuide.configuration(),
      DocGuide.monitoring()
    ]
  end

  def help_section(), do: :configuration

  def description(), do: "Displays the name and value of each variable in the application environment for each running application"

  def banner(_, %{node: node_name}), do: "Application environment of node #{node_name} ..."
end
