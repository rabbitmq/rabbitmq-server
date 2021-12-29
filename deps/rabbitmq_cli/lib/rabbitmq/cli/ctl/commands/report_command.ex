## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.

defmodule RabbitMQ.CLI.Ctl.Commands.ReportCommand do
  alias RabbitMQ.CLI.Core.DocGuide
  alias RabbitMQ.CLI.Ctl.Commands.{
    ClusterStatusCommand,
    EnvironmentCommand,
    ListBindingsCommand,
    ListChannelsCommand,
    ListConnectionsCommand,
    ListExchangesCommand,
    ListGlobalParametersCommand,
    ListParametersCommand,
    ListPermissionsCommand,
    ListPoliciesCommand,
    ListQueuesCommand,
    StatusCommand
  }
  alias RabbitMQ.CLI.Diagnostics.Commands.{
    CommandLineArgumentsCommand,
    OsEnvCommand
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
    case :rabbit_misc.rpc_call(node_name, :rabbit_vhost, :list_names, []) do
      {:badrpc, _} = err ->
        err

      vhosts ->
        data = [
          run_command(StatusCommand, [], opts),
          run_command(ClusterStatusCommand, [], opts),
          run_command(EnvironmentCommand, [], opts),
          run_command(ListConnectionsCommand, info_keys(ListConnectionsCommand), opts),
          run_command(ListChannelsCommand, info_keys(ListChannelsCommand), opts),
          run_command(CommandLineArgumentsCommand, [], opts),
          run_command(OsEnvCommand, [], opts)
        ]

        vhost_data =
          vhosts
          |> Enum.flat_map(fn v ->
            opts = Map.put(opts, :vhost, v)

            [
              run_command(ListQueuesCommand, info_keys(ListQueuesCommand), opts),
              run_command(ListExchangesCommand, info_keys(ListExchangesCommand), opts),
              run_command(ListBindingsCommand, info_keys(ListBindingsCommand), opts),
              run_command(ListPermissionsCommand, [], opts),
              run_command(ListPoliciesCommand, [], opts),
              run_command(ListGlobalParametersCommand, [], opts),
              run_command(ListParametersCommand, [], opts),

            ]
          end)

        data ++ vhost_data
    end
  end

  use RabbitMQ.CLI.DefaultOutput

  def formatter(), do: RabbitMQ.CLI.Formatters.Report

  def usage, do: "report"

  def usage_doc_guides() do
    [
      DocGuide.monitoring()
    ]
  end

  def help_section(), do: :observability_and_health_checks

  def description(), do: "Generate a server status report containing a concatenation of all server status information for support purposes"

  def banner(_, %{node: node_name}), do: "Reporting server status of node #{node_name} ..."

  #
  # Implementation
  #

  defp run_command(command, args, opts) do
    {args, opts} = command.merge_defaults(args, opts)
    banner = command.banner(args, opts)
    command_result = command.run(args, opts) |> command.output(opts)
    {command, banner, command_result}
  end

  defp info_keys(command) do
    command.info_keys()
    |> Enum.map(&to_string/1)
  end
end
