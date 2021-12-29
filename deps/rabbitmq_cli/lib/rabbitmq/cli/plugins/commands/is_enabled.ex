## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.

defmodule RabbitMQ.CLI.Plugins.Commands.IsEnabledCommand do
  alias RabbitMQ.CLI.Plugins.Helpers, as: PluginHelpers
  alias RabbitMQ.CLI.Core.{DocGuide, Validators}
  import RabbitMQ.CLI.Core.{CodePath, Paths}

  @behaviour RabbitMQ.CLI.CommandBehaviour

  def merge_defaults(args, %{offline: true} = opts) do
    {args, opts}
  end

  def merge_defaults(args, opts) do
    {args, Map.merge(%{online: true, offline: false}, opts)}
  end

  def distribution(%{offline: true}), do: :none
  def distribution(%{offline: false}), do: :cli

  def switches(), do: [online: :boolean, offline: :boolean]

  def validate(_, %{online: true, offline: true}) do
    {:validation_failure, {:bad_argument, "Cannot set both online and offline"}}
  end

  def validate(_, %{online: false, offline: false}) do
    {:validation_failure, {:bad_argument, "Cannot set online and offline to false"}}
  end

  def validate([], _) do
    {:validation_failure, :not_enough_args}
  end

  def validate([_ | _], _) do
    :ok
  end

  def validate_execution_environment(args, %{offline: true} = opts) do
    Validators.chain(
      [
        &require_rabbit_and_plugins/2,
        &PluginHelpers.enabled_plugins_file/2,
        &plugins_dir/2
      ],
      [args, opts]
    )
  end

  def validate_execution_environment(args, %{online: true} = opts) do
    Validators.node_is_running(args, opts)
  end

  def run(args, %{online: true, node: node_name} = opts) do
    case :rabbit_misc.rpc_call(node_name, :rabbit_plugins, :active, []) do
      {:error, _} = e ->
        e

      plugins ->
        plugins = Enum.map(plugins, &Atom.to_string/1) |> Enum.sort()

        case Enum.filter(args, fn x -> not Enum.member?(plugins, x) end) do
          [] -> {:ok, positive_result_message(args, opts)}
          xs -> {:error, negative_result_message(xs, opts, plugins)}
        end
    end
  end

  def run(args, %{offline: true} = opts) do
    plugins = PluginHelpers.list_names(opts) |> Enum.map(&Atom.to_string/1) |> Enum.sort()

    case Enum.filter(args, fn x -> not Enum.member?(plugins, x) end) do
      [] -> {:ok, positive_result_message(args, opts)}
      xs -> {:error, negative_result_message(xs, opts, plugins)}
    end
  end

  def output({:ok, msg}, %{formatter: "json"}) do
    {:ok, %{"result" => "ok", "message" => msg}}
  end

  def output({:error, msg}, %{formatter: "json"}) do
    {:error, RabbitMQ.CLI.Core.ExitCodes.exit_unavailable(),
     %{"result" => "error", "message" => msg}}
  end

  def output({:error, err}, _opts) do
    {:error, RabbitMQ.CLI.Core.ExitCodes.exit_unavailable(), err}
  end

  use RabbitMQ.CLI.DefaultOutput

  def usage, do: "is_enabled <plugin1> [ <plugin2>] [--offline] [--online]"

  def usage_additional() do
    [
      ["<plugin1> [ <plugin2>]", "names of plugins to check separated by a space"],
      ["--online", "contact target node to perform the check. Requires the node to be running and reachable."],
      ["--offline", "check enabled plugins file directly without contacting target node."]
    ]
  end

  def usage_doc_guides() do
    [
      DocGuide.plugins()
    ]
  end

  def banner(args, %{offline: true}) do
    "Inferring if #{plugin_or_plugins(args)} from local environment..."
  end

  def banner(args, %{online: true, node: node}) do
    "Asking node #{node} if #{plugin_or_plugins(args)} enabled..."
  end

  def help_section(), do: :observability_and_health_checks

  def description(), do: "Health check that exits with a non-zero code if provided plugins are not enabled on target node"

  #
  # Implementation
  #

  def plugin_or_plugins(args) when length(args) == 1 do
    "plugin #{PluginHelpers.comma_separated_names(args)} is"
  end

  def plugin_or_plugins(args) when length(args) > 1 do
    "plugins #{PluginHelpers.comma_separated_names(args)} are"
  end

  defp positive_result_message(args, %{online: true, node: node_name}) do
    String.capitalize("#{plugin_or_plugins(args)} enabled on node #{node_name}")
  end

  defp positive_result_message(args, %{offline: true}) do
    String.capitalize("#{plugin_or_plugins(args)} enabled")
  end

  defp negative_result_message(missing, %{online: true, node: node_name}, plugins) do
    String.capitalize("#{plugin_or_plugins(missing)} not enabled on node #{node_name}. ") <>
      "Enabled plugins and dependencies: #{PluginHelpers.comma_separated_names(plugins)}"
  end

  defp negative_result_message(missing, %{offline: true}, plugins) do
    String.capitalize("#{plugin_or_plugins(missing)} not enabled. ") <>
      "Enabled plugins and dependencies: #{PluginHelpers.comma_separated_names(plugins)}"
  end
end
