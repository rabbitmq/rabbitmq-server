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
## Copyright (c) 2007-2016 Pivotal Software, Inc.  All rights reserved.


defmodule RabbitMQ.CLI.Plugins.Commands.EnableCommand do

  alias RabbitMQ.CLI.Plugins.Helpers, as: PluginHelpers
  alias RabbitMQ.CLI.Core.Helpers, as: Helpers

  @behaviour RabbitMQ.CLI.CommandBehaviour

  def formatter(), do: RabbitMQ.CLI.Formatters.Plugins

  def merge_defaults(args, opts) do
    {args, Map.merge(%{online: false, offline: false}, opts)}
  end

  def switches(), do: [online: :boolean,
                       offline: :boolean]

  def validate([], _) do
    {:validation_failure, :not_enough_arguments}
  end
  def validate(_, %{online: true, offline: true}) do
    {:validation_failure, {:bad_argument, "Cannot set both online and offline"}}
  end

  def validate(_, opts) do
    :ok
    |> validate_step(fn() -> Helpers.require_rabbit(opts) end)
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

  def usage, do: "enable [<plugin>] [--offline] [--online]"

  def banner(plugins, %{node: node_name}) do
    ["Enabling plugins on node #{node_name}:" | plugins]
  end


  def run(plugin_names, %{node: node_name} = opts) do
    plugins = for s <- plugin_names, do: String.to_atom(s)
    %{online: online, offline: offline} = opts

    enabled = PluginHelpers.read_enabled(opts)
    all     = PluginHelpers.list(opts)
    implicit           = :rabbit_plugins.dependencies(false, enabled, all)
    enabled_implicitly = MapSet.difference(MapSet.new(implicit), MapSet.new(enabled))

    plugins_to_set = MapSet.union(
      MapSet.new(enabled),
      MapSet.difference(MapSet.new(plugins), enabled_implicitly))

    mode = case {online, offline} do
             {true, false}  -> :online;
             {false, true}  -> :offline;
             # fallback to online mode
             {false, false} -> :online
           end

    case PluginHelpers.set_enabled_plugins(MapSet.to_list(plugins_to_set), opts) do
      {:ok, enabled_plugins} ->
        {:stream, Stream.concat(
            [[enabled_plugins],
             RabbitMQ.CLI.Core.Helpers.defer(
               fn() ->
                 case PluginHelpers.update_enabled_plugins(enabled_plugins, mode,
                       node_name, opts) do
                   %{set: new_enabled} = result ->
                     enabled = new_enabled -- implicit
                     Map.put(result, :enabled, enabled);
                   other -> other
                 end
               end)])};
      {:error, _} = err ->
        err
    end
  end

  def output({:error, err}, _opts) do
    {:error, err}
  end
  def output({:stream, stream}, _opts) do
    {:stream, stream}
  end

end
