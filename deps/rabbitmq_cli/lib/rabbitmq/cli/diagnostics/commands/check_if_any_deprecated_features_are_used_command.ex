## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2023 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries.  All rights reserved.

defmodule RabbitMQ.CLI.Diagnostics.Commands.CheckIfAnyDeprecatedFeaturesAreUsedCommand do
  alias RabbitMQ.CLI.Core.DocGuide

  alias RabbitMQ.CLI.Diagnostics.Commands.{
    CheckIfClusterHasClassicQueueMirroringPolicyCommand
  }

  @behaviour RabbitMQ.CLI.CommandBehaviour

  def scopes(), do: [:ctl, :diagnostics]

  use RabbitMQ.CLI.Core.AcceptsDefaultSwitchesAndTimeout
  use RabbitMQ.CLI.Core.MergesNoDefaults
  use RabbitMQ.CLI.Core.AcceptsNoPositionalArguments
  use RabbitMQ.CLI.Core.RequiresRabbitAppRunning

  def run([], opts) do
    are_deprecated_features_used = %{
      :classic_queue_mirroring => is_used_classic_queue_mirroring(opts)
    }

    deprecated_features_list =
      Enum.reduce(
        are_deprecated_features_used,
        [],
        fn
          {_feat, _result}, {:badrpc, _} = acc ->
            acc

          {feat, result}, acc ->
            case result do
              {:badrpc, _} = err -> err
              {:error, _} = err -> err
              true -> [feat | acc]
              false -> acc
            end
        end
      )

    # health checks return true if they pass
    case deprecated_features_list do
      {:badrpc, _} = err -> err
      {:error, _} = err -> err
      [] -> true
      xs when is_list(xs) -> {false, deprecated_features_list}
    end
  end

  def is_used_classic_queue_mirroring(%{node: node_name, timeout: timeout}) do
    :rabbit_misc.rpc_call(
      node_name,
      :rabbit_mirror_queue_misc,
      :are_cmqs_used,
      [:none],
      timeout
    )
  end

  def output(true, %{formatter: "json"}) do
    {:ok, %{"result" => "ok"}}
  end

  def output(true, %{silent: true}) do
    {:ok, :check_passed}
  end

  def output(true, %{}) do
    {:ok, "Cluster reported no deprecated features in use"}
  end

  def output({false, deprecated_features_list}, %{formatter: "json"}) do
    {:error, :check_failed,
     %{
       "result" => "error",
       "deprecated_features" => deprecated_features_list,
       "message" => "Cluster reported deprecated features in use"
     }}
  end

  def output({false, _deprecated_features_list}, %{silent: true}) do
    {:error, :check_failed}
  end

  def output({false, deprecated_features_list}, _) do
    {:error, :check_failed, deprecated_features_list}
  end

  use RabbitMQ.CLI.DefaultOutput

  def usage, do: "check_if_any_deprecated_features_are_used"

  def help_section(), do: :observability_and_health_checks

  def description(),
    do: "Generate a report listing all deprecated features in use"

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
