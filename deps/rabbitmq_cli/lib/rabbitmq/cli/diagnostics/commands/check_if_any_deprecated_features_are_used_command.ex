## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2023 VMware, Inc. or its affiliates.  All rights reserved.

defmodule RabbitMQ.CLI.Diagnostics.Commands.CheckIfAnyDeprecatedFeaturesAreUsedCommand do
  alias RabbitMQ.CLI.Core.DocGuide

  alias RabbitMQ.CLI.Diagnostics.Commands.{
    CheckIfClusterHasClassicQueueMirroringPolicyCommand
  }

  @behaviour RabbitMQ.CLI.CommandBehaviour

  def scopes(), do: [:ctl, :diagnostics]

  use RabbitMQ.CLI.Core.MergesNoDefaults

  def validate([_ | _] = args, _) when length(args) != 0,
    do: {:validation_failure, :too_many_args}

  def validate([], %{formatter: formatter}) do
    case formatter do
      "report" -> :ok
      _other -> {:validation_failure, "Only report formatter is supported"}
    end
  end

  def validate([], _), do: :ok

  use RabbitMQ.CLI.Core.RequiresRabbitAppRunning

  def run([], %{node: node_name} = opts) do
        [
          run_command(CheckIfClusterHasClassicQueueMirroringPolicyCommand, [], opts)
        ]
  end

  use RabbitMQ.CLI.DefaultOutput

  def formatter(), do: RabbitMQ.CLI.Formatters.Report

  def usage, do: "check_if_any_deprecated_features_are_used"

  def help_section(), do: :observability_and_health_checks

  def description(),
    do:
      "Generate a report listing all deprecated features in use"

  def banner(_, %{node: node_name}), do: "Checking if any deprecated features are used ..."

  #
  # Implementation
  #

  defp run_command(command, args, opts) do
    {args, opts} = command.merge_defaults(args, opts)
    banner = command.banner(args, opts)
    command_result = command.run(args, opts) |> command.output(opts)
    {command, banner, command_result}
  end

end
