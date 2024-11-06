## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2023 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries.  All rights reserved.

defmodule RabbitMQ.CLI.Diagnostics.Commands.CheckIfAnyDeprecatedFeaturesAreUsedCommand do
  @behaviour RabbitMQ.CLI.CommandBehaviour

  def scopes(), do: [:ctl, :diagnostics]

  use RabbitMQ.CLI.Core.AcceptsDefaultSwitchesAndTimeout
  use RabbitMQ.CLI.Core.MergesNoDefaults
  use RabbitMQ.CLI.Core.AcceptsNoPositionalArguments
  use RabbitMQ.CLI.Core.RequiresRabbitAppRunning

  def run([], %{node: node_name, timeout: timeout}) do
    deprecated_features_list = :rabbit_misc.rpc_call(
      node_name,
      :rabbit_deprecated_features,
      :list,
      [:used],
      timeout
    )

    # health checks return true if they pass
    case deprecated_features_list do
      {:badrpc, _} = err ->
        err
      {:error, _} = err ->
        err
      _ ->
        names = Enum.sort(Map.keys(deprecated_features_list))
        case names do
          [] -> true
          _  -> {false, names}
        end
    end
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

  def banner(_, %{node: _node_name}), do: "Checking if any deprecated features are used ..."
end
