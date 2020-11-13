## The contents of this file are subject to the Mozilla Public License
## Version 1.1 (the "License"); you may not use this file except in
## compliance with the License. You may obtain a copy of the License
## at https://www.mozilla.org/MPL/
##
## Software distributed under the License is distributed on an "AS IS"
## basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
## the License for the specific language governing rights and
## limitations under the License.
##
## The Original Code is RabbitMQ.
##
## The Initial Developer of the Original Code is GoPivotal, Inc.
## Copyright (c) 2007-2019 Pivotal Software, Inc.  All rights reserved.

defmodule RabbitMQ.CLI.Plugins.Commands.SetCommand do
  alias RabbitMQ.CLI.Plugins.Helpers, as: PluginHelpers
  alias RabbitMQ.CLI.Core.{DocGuide, ExitCodes, Validators}
  import RabbitMQ.CLI.Core.{CodePath, Paths}

  @behaviour RabbitMQ.CLI.CommandBehaviour

  def formatter(), do: RabbitMQ.CLI.Formatters.Plugins

  def merge_defaults(args, opts) do
    {args, Map.merge(%{online: false, offline: false}, opts)}
  end

  def distribution(%{offline: true}), do: :none
  def distribution(%{offline: false}), do: :cli

  def switches(), do: [online: :boolean, offline: :boolean]

  def validate(_, %{online: true, offline: true}) do
    {:validation_failure, {:bad_argument, "Cannot set both online and offline"}}
  end

  def validate(_, _) do
    :ok
  end

  def validate_execution_environment(args, opts) do
    Validators.chain(
      [
        &PluginHelpers.can_set_plugins_with_mode/2,
        &require_rabbit_and_plugins/2,
        &PluginHelpers.enabled_plugins_file/2,
        &plugins_dir/2
      ],
      [args, opts]
    )
  end

  def run(plugin_names, opts) do
    plugins = for s <- plugin_names, do: String.to_atom(s)

    case PluginHelpers.validate_plugins(plugins, opts) do
      :ok -> do_run(plugins, opts)
      other -> other
    end
  end

  def output({:error, {:plugins_not_found, missing}}, _opts) do
    {:error, ExitCodes.exit_dataerr(),
     "The following plugins were not found: #{Enum.join(Enum.to_list(missing), ", ")}"}
  end

  use RabbitMQ.CLI.Plugins.ErrorOutput

  def banner(plugins, %{node: node_name}) do
    ["Enabling plugins on node #{node_name}:" | plugins]
  end

  def usage, do: "set <plugin1> [ <plugin2>] [--offline] [--online]"

  def usage_additional() do
    [
      ["<plugin1> [ <plugin2>]", "names of plugins to enable separated by a space. All other plugins will be disabled."],
      ["--online", "contact target node to enable the plugins. Changes are applied immediately."],
      ["--offline", "update enabled plugins file directly without contacting target node. Changes will be delayed until the node is restarted."]
    ]
  end

  def usage_doc_guides() do
    [
      DocGuide.plugins()
    ]
  end

  def help_section(), do: :plugin_management

  def description(), do: "Enables one or more plugins, disables the rest"

  #
  # Implementation
  #

  def do_run(plugins, %{node: node_name} = opts) do
    all = PluginHelpers.list(opts)
    mode = PluginHelpers.mode(opts)

    case PluginHelpers.set_enabled_plugins(plugins, opts) do
      {:ok, enabled_plugins} ->
        {:stream,
         Stream.concat([
           [:rabbit_plugins.strictly_plugins(enabled_plugins, all)],
           RabbitMQ.CLI.Core.Helpers.defer(fn ->
             case PluginHelpers.update_enabled_plugins(
                    enabled_plugins,
                    mode,
                    node_name,
                    opts
                  ) do
               %{set: _} = map ->
                 filter_strictly_plugins(map, all, [:set, :started, :stopped])

               {:error, _} = err ->
                 err
             end
           end)
         ])}

      {:error, _} = err ->
        err
    end
  end

  defp filter_strictly_plugins(map, _all, []) do
    map
  end

  defp filter_strictly_plugins(map, all, [head | tail]) do
    case map[head] do
      nil ->
        filter_strictly_plugins(map, all, tail)

      other ->
        value = :rabbit_plugins.strictly_plugins(other, all)
        filter_strictly_plugins(Map.put(map, head, value), all, tail)
    end
  end
end
