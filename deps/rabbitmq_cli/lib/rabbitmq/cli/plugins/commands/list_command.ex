## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.

defmodule RabbitMQ.CLI.Plugins.Commands.ListCommand do
  import RabbitCommon.Records

  alias RabbitMQ.CLI.Core.{DocGuide, Validators}
  alias RabbitMQ.CLI.Plugins.Helpers, as: PluginHelpers
  import RabbitMQ.CLI.Core.{CodePath, Paths}

  @behaviour RabbitMQ.CLI.CommandBehaviour
  use RabbitMQ.CLI.DefaultOutput

  def formatter(), do: RabbitMQ.CLI.Formatters.Plugins

  def merge_defaults([], opts), do: merge_defaults([".*"], opts)
  def merge_defaults(args, opts), do: {args, Map.merge(default_opts(), opts)}

  def switches(),
    do: [verbose: :boolean, minimal: :boolean, enabled: :boolean, implicitly_enabled: :boolean]

  def aliases(), do: [v: :verbose, m: :minimal, E: :enabled, e: :implicitly_enabled]

  def validate(args, _) when length(args) > 1 do
    {:validation_failure, :too_many_args}
  end

  def validate(_, %{verbose: true, minimal: true}) do
    {:validation_failure, {:bad_argument, "Cannot set both verbose and minimal"}}
  end

  def validate(_, _) do
    :ok
  end

  def validate_execution_environment(args, opts) do
    Validators.chain(
      [
        &require_rabbit_and_plugins/2,
        &PluginHelpers.enabled_plugins_file/2,
        &plugins_dir/2
      ],
      [args, opts]
    )
  end

  def run([pattern], %{node: node_name} = opts) do
    %{verbose: verbose, minimal: minimal, enabled: only_enabled, implicitly_enabled: all_enabled} =
      opts

    all = PluginHelpers.list(opts)
    enabled = PluginHelpers.read_enabled(opts)

    missing = MapSet.difference(MapSet.new(enabled), MapSet.new(PluginHelpers.plugin_names(all)))

    case Enum.empty?(missing) do
      true ->
        :ok

      false ->
        names = Enum.join(Enum.to_list(missing), ", ")
        IO.puts("WARNING - plugins currently enabled but missing: #{names}\n")
    end

    implicit = :rabbit_plugins.dependencies(false, enabled, all)
    enabled_implicitly = implicit -- enabled

    {status, running} =
      case remote_running_plugins(node_name) do
        :error -> {:node_down, []}
        {:ok, active} -> {:running, active}
      end

    {:ok, re} = Regex.compile(pattern)

    format =
      case {verbose, minimal} do
        {true, false} -> :verbose
        {false, true} -> :minimal
        {false, false} -> :normal
      end

    plugins =
      Enum.filter(
        all,
        fn plugin ->
          name = PluginHelpers.plugin_name(plugin)

          :rabbit_plugins.is_strictly_plugin(plugin) and
            Regex.match?(re, to_string(name)) and
            cond do
              only_enabled -> Enum.member?(enabled, name)
              all_enabled -> Enum.member?(enabled ++ enabled_implicitly, name)
              true -> true
            end
        end
      )

    %{
      status: status,
      format: format,
      plugins: format_plugins(plugins, format, enabled, enabled_implicitly, running)
    }
  end

  def banner([pattern], _), do: "Listing plugins with pattern \"#{pattern}\" ..."

  def usage, do: "list [pattern] [--verbose] [--minimal] [--enabled] [--implicitly-enabled]"

  def usage_additional() do
    [
      ["<pattern>", "only list plugins that match a regular expression pattern"],
      ["--verbose", "output more information"],
      ["--minimal", "only print plugin names. Most useful in compbination with --silent and --enabled."],
      ["--enabled", "only list enabled plugins"],
      ["--implicitly-enabled", "include plugins enabled as dependencies of other plugins"]
    ]
  end

  def usage_doc_guides() do
    [
      DocGuide.plugins()
    ]
  end

  def help_section(), do: :plugin_management

  def description(), do: "Lists plugins and their state"

  #
  # Implementation
  #

  defp remote_running_plugins(node) do
    case :rabbit_misc.rpc_call(node, :rabbit_plugins, :running_plugins, []) do
      {:badrpc, _} -> :error
      active_with_version -> active_with_version
    end
  end

  defp format_plugins(plugins, format, enabled, enabled_implicitly, running) do
    plugins
    |> sort_plugins
    |> Enum.map(fn plugin ->
      format_plugin(plugin, format, enabled, enabled_implicitly, running)
    end)
  end

  defp sort_plugins(plugins) do
    Enum.sort_by(plugins, &PluginHelpers.plugin_name/1)
  end

  defp format_plugin(plugin, :minimal, _, _, _) do
    %{name: PluginHelpers.plugin_name(plugin)}
  end

  defp format_plugin(plugin, :normal, enabled, enabled_implicitly, running) do
    plugin(name: name, version: version) = plugin

    enabled_mode =
      case {Enum.member?(enabled, name), Enum.member?(enabled_implicitly, name)} do
        {true, false} -> :enabled
        {false, true} -> :implicit
        {false, false} -> :not_enabled
      end

    %{
      name: name,
      version: version,
      running_version: running[name],
      enabled: enabled_mode,
      running: Keyword.has_key?(running, name)
    }
  end

  defp format_plugin(plugin, :verbose, enabled, enabled_implicitly, running) do
    normal = format_plugin(plugin, :normal, enabled, enabled_implicitly, running)
    plugin(dependencies: dependencies, description: description) = plugin
    Map.merge(normal, %{dependencies: dependencies, description: description})
  end

  defp default_opts() do
    %{minimal: false, verbose: false, enabled: false, implicitly_enabled: false}
  end
end
