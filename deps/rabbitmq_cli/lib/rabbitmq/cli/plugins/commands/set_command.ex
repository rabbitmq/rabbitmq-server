## The contents of this file are subject to the Mozilla Public License
## Version 1.1 (the "License"); you may not use this file except in
## compliance with the License. You may obtain a copy of the License
## at http://www.mozilla.org/MPL/
##
## Software distributed under the License is distributed on an "AS IS"
## basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
## the License for the specific language governing rights and
## limitations under the License.
##
## The Original Code is RabbitMQ.
##
## The Initial Developer of the Original Code is GoPivotal, Inc.
## Copyright (c) 2007-2017 Pivotal Software, Inc.  All rights reserved.


defmodule RabbitMQ.CLI.Plugins.Commands.SetCommand do

  alias RabbitMQ.CLI.Plugins.Helpers, as: PluginHelpers
  alias RabbitMQ.CLI.Core.Helpers, as: Helpers
  alias RabbitMQ.CLI.Core.ExitCodes, as: ExitCodes

  @behaviour RabbitMQ.CLI.CommandBehaviour

  def formatter(), do: RabbitMQ.CLI.Formatters.Plugins

  def merge_defaults(args, opts) do
    {args, Map.merge(%{online: false, offline: false}, opts)}
  end

  def switches(), do: [online: :boolean,
                       offline: :boolean]

  def validate(_, %{online: true, offline: true}) do
    {:validation_failure, {:bad_argument, "Cannot set both online and offline"}}
  end

  def validate(_plugins, opts) do
    :ok
    |> validate_step(fn() -> Helpers.require_rabbit_and_plugins(opts) end)
    |> validate_step(fn() -> PluginHelpers.enabled_plugins_file(opts) end)
    |> validate_step(fn() -> Helpers.plugins_dir(opts) end)
  end

  def validate_step(:ok, step) do
    case step.() do
      {:error, err} -> {:validation_failure, err};
      _             -> :ok
    end
  end
  def validate_step({:validation_failure, err}, _) do
    {:validation_failure, err}
  end

  def usage, do: "set [<plugin>] [--offline] [--online]"

  def banner(plugins, %{node: node_name}) do
      ["Enabling plugins on node #{node_name}:" | plugins]
  end


  def run(plugin_names, opts) do
    plugins = for s <- plugin_names, do: String.to_atom(s)
    case PluginHelpers.validate_plugins(plugins, opts) do
      :ok   -> do_run(plugins, opts)
      other -> other
    end
  end

  def do_run(plugins, %{node: node_name} = opts) do
    %{online: online, offline: offline} = opts

    all = PluginHelpers.list(opts)
    mode = case {online, offline} do
      {true, false}  -> :online;
      {false, true}  -> :offline;
      {false, false} -> :online
    end

    case PluginHelpers.set_enabled_plugins(plugins, opts) do
      {:ok, enabled_plugins} ->
        {:stream, Stream.concat(
            [[:rabbit_plugins.strictly_plugins(enabled_plugins, all)],
             RabbitMQ.CLI.Core.Helpers.defer(
               fn() ->
                 map = PluginHelpers.update_enabled_plugins(enabled_plugins, mode, node_name, opts)
                 filter_strictly_plugins(map, all, [:set, :started, :stopped])
               end)])};
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
        filter_strictly_plugins(map, all, tail);
      other ->
        value = :rabbit_plugins.strictly_plugins(other, all)
        filter_strictly_plugins(Map.put(map, head, value), all, tail)
    end
  end

  def output({:error, err}, _opts) do
    {:error, ExitCodes.exit_software(), to_string(err)}
  end
  def output({:stream, stream}, _opts) do
    {:stream, stream}
  end

end
