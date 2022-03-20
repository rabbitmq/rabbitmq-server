## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.

defmodule RabbitMQ.CLI.Ctl.Commands.ClearGlobalParameterCommand do
  alias RabbitMQ.CLI.Core.{DocGuide, Helpers}

  @behaviour RabbitMQ.CLI.CommandBehaviour

  use RabbitMQ.CLI.Core.MergesNoDefaults
  use RabbitMQ.CLI.Core.AcceptsOnePositionalArgument
  use RabbitMQ.CLI.Core.RequiresRabbitAppRunning

  def run([key], %{node: node_name}) do
    :rabbit_misc.rpc_call(
      node_name,
      :rabbit_runtime_parameters,
      :clear_global,
      [key, Helpers.cli_acting_user()]
    )
  end

  use RabbitMQ.CLI.DefaultOutput

  def usage, do: "clear_global_parameter <name>"

  def usage_additional() do
    [
      ["<name>", "parameter name (identifier)"]
    ]
  end

  def usage_doc_guides() do
    [
      DocGuide.parameters()
    ]
  end

  def help_section(), do: :parameters

  def description(), do: "Clears a global runtime parameter"

  def banner([key], _) do
    "Clearing global runtime parameter \"#{key}\" ..."
  end
end
