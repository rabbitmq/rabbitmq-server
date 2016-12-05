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

alias RabbitMQ.CLI.Core.CommandModules, as: CommandModules

defmodule RabbitMQ.CLI.Core.Parser do

  @levenshtein_distance_limit 5

  @spec parse(String.t) :: {command :: :no_command | atom(),
                            command_name :: String.t,
                            arguments :: [String.t],
                            options :: Map.t,
                            invalid :: [{String.t, String.t | nil}]}

  def parse(input) do
    {parsed_args, options, invalid} = parse_global(input)
    CommandModules.load(options)

    {command_name, command_module, arguments} = look_up_command(parsed_args)

    case command_module do
      nil ->
        {:no_command, command_name, arguments, options, invalid};
      {:suggest, _} = suggest ->
        {suggest, command_name, arguments, options, invalid};
      command_module when is_atom(command_module) ->
        {[^command_name | cmd_arguments], cmd_options, cmd_invalid} =
          parse_command_specific(input, command_module)
        {command_module, command_name, cmd_arguments, cmd_options, cmd_invalid}
    end
  end

  defp look_up_command(parsed_args) do
    case parsed_args do
      [cmd_name | arguments] ->
        module_map = CommandModules.module_map
        command = case module_map[cmd_name] do
          nil     -> closest_similar_command(cmd_name, module_map)
          command -> command
        end
        {cmd_name, command, arguments}
      [] ->
        {"", nil, []}
    end
  end

  defp closest_similar_command(_cmd_name, empty) when empty == %{} do
    nil
  end
  defp closest_similar_command(cmd_name, module_map) do
    suggestion = module_map
                 |> Map.keys
                 |> Enum.map(fn(cmd) ->
                      {cmd, Simetric.Levenshtein.compare(cmd, cmd_name)}
                    end)
                 |> Enum.min_by(fn({_,distance}) -> distance end)
    case suggestion do
      {cmd, distance} when distance < @levenshtein_distance_limit ->
        {:suggest, cmd};
      _ ->
        nil
    end
  end

  def parse_command_specific(input, command) do
    switches = build_switches(default_switches(), command)
    aliases = build_aliases(default_aliases(), command)
    parse_generic(input, switches, aliases)
  end

  def parse_global(input) do
    switches = default_switches()
    aliases = default_aliases()
    parse_generic(input, switches, aliases)
  end

  defp parse_generic(input, switches, aliases) do
    {options, args, invalid} = OptionParser.parse(
      input,
      strict: switches,
      aliases: aliases
    )
    norm_options = normalize_options(options, switches) |> Map.new
    {args, norm_options, invalid}
  end

  def default_switches() do
    [node: :atom,
     quiet: :boolean,
     dry_run: :boolean,
     timeout: :integer,
     vhost: :string,
     longnames: :boolean,
     formatter: :string,
     printer: :string,
     file: :string,
     script_name: :atom,
     rabbitmq_home: :string,
     mnesia_dir: :string,
     plugins_dir: :string,
     enabled_plugins_file: :string
    ]
  end

  def default_aliases() do
    [p: :vhost,
     n: :node,
     q: :quiet,
     t: :timeout,
     l: :longnames]
  end

  defp build_switches(default, command) do
    command_switches = apply_if_exported(command, :switches, [], [])
    merge_if_different(default, command_switches,
                       {:command_invalid,
                        {command, {:invalid_switches,
                                   command_switches}}})
  end

  defp build_aliases(default, command) do
    command_aliases = apply_if_exported(command, :aliases, [], [])
    merge_if_different(default, command_aliases,
                       {:command_invalid,
                        {command, {:invalid_aliases,
                                   command_aliases}}})
  end

  defp apply_if_exported(mod, fun, args, default) do
    case function_exported?(mod, fun, length(args)) do
      true  -> apply(mod, fun, args);
      false -> default
    end
  end

  defp merge_if_different(default, specific, error) do
    case keyword_intersect(default, specific) do
      [] -> Keyword.merge(default, specific);
      _  -> exit(error)
    end
  end

  defp keyword_intersect(one, two) do
    one_keys = MapSet.new(Keyword.keys(one))
    two_keys = MapSet.new(Keyword.keys(two))
    case MapSet.intersection(one_keys, two_keys) do
      %MapSet{} -> [];
      set       -> MapSet.to_list(set)
    end
  end

  defp normalize_options(options, switches) do
    Enum.map(options,
             fn({key, option}) ->
               {key, normalize_type(option, switches[key])}
             end)
  end

  defp normalize_type(value, :atom) when is_binary(value) do
    String.to_atom(value)
  end
  defp normalize_type(value, _type) do
    value
  end

end
