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

alias RabbitMQ.CLI.Core.{CommandModules, Config}

defmodule RabbitMQ.CLI.Core.Parser do

  # This assumes the average word letter count in
  # the English language is 5.
  @levenshtein_distance_limit 5

  @spec parse([String.t]) :: {command :: :no_command | atom() | {:suggest, String.t},
                              command_name :: String.t,
                              arguments :: [String.t],
                              options :: map(),
                              invalid :: [{String.t, String.t | nil}]}

  def parse(input) do
    {parsed_args, options, invalid} = parse_global(input)
    {command_name, command_module, arguments} = look_up_command(parsed_args, options)

    case command_module do
      nil ->
        {:no_command, command_name, arguments, options, invalid};
      {:suggest, _} = suggest ->
        {suggest, command_name, arguments, options, invalid};
      {:alias, alias_module, alias_content} ->
        {[_alias_command_name | cmd_arguments], cmd_options, cmd_invalid} =
          parse_alias(input, command_name, alias_module, alias_content)
        {alias_module, command_name, cmd_arguments, cmd_options, cmd_invalid};
      command_module when is_atom(command_module) ->
        {[^command_name | cmd_arguments], cmd_options, cmd_invalid} =
          parse_command_specific(input, command_module)
        {command_module, command_name, cmd_arguments, cmd_options, cmd_invalid}
    end
  end

  defp look_up_command(parsed_args, options) do
    case parsed_args do
      [cmd_name | arguments] ->
        ## This is an optimisation for pluggable command discovery.
        ## Most of the time a command will be from rabbitmqctl application
        ## so there is not point in scanning plugins for potential commands
        CommandModules.load_core(options)
        core_commands = CommandModules.module_map_core
        command = case core_commands[cmd_name] do
          nil ->
            CommandModules.load(options)
            module_map = CommandModules.module_map
            module_map[cmd_name] ||
              command_alias(cmd_name, module_map, options) ||
              command_suggestion(cmd_name, module_map);
          c -> c
        end
        {cmd_name, command, arguments}
      [] ->
        {"", nil, []}
    end
  end

  defp command_alias(cmd_name, module_map, options) do
    aliases = load_aliases(options)
    case aliases[cmd_name] do
      nil -> nil;
      [alias_cmd_name | _] = alias_content ->
        case module_map[alias_cmd_name] do
          nil -> nil;
          module -> {:alias, module, alias_content}
        end
    end
  end

  defp load_aliases(options) do
    aliases_file = Config.get_option(:aliases_file, options)
    case aliases_file && File.read(aliases_file) do
      ## No aliases file
      nil -> %{};
      {:ok, content} ->
        String.split(content, "\n")
        |>  Enum.reduce(%{},
              fn(str, acc) ->
                case String.split(str, "=", parts: 2) do
                  [alias_name, alias_string] ->
                    Map.put(acc, String.trim(alias_name), OptionParser.split(alias_string))
                  _ ->
                    acc
                end
            end);
      {:error, err} ->
        IO.puts(:stderr, "Error reading aliases file #{aliases_file}: #{err}")
        %{}
    end
  end

  defp command_suggestion(_cmd_name, empty) when empty == %{} do
    nil
  end
  defp command_suggestion(cmd_name, module_map) do
    suggestion = module_map
                 |> Map.keys
                 |> Enum.map(fn(cmd) ->
                      {cmd, Simetric.Levenshtein.compare(cmd, cmd_name)}
                    end)
                 |> Enum.min_by(fn({_,distance}) -> distance end)
    case suggestion do
      {cmd, distance} when distance <= @levenshtein_distance_limit ->
        {:suggest, cmd};
      _ ->
        nil
    end
  end

  def parse_alias(input, command_name, module, alias_content) do
    {pre_command_options, tail, invalid} = parse_global_head(input)
    [^command_name | other] = tail
    aliased_input = alias_content ++ other
    {args, options, command_invalid} = parse_command_specific(aliased_input, module)
    merged_options = Map.merge(options, pre_command_options)
    {args, merged_options, command_invalid ++ invalid}
  end

  def parse_command_specific(input, command) do
    switches = build_switches(default_switches(), command)
    aliases = build_aliases(default_aliases(), command)
    parse_generic(input, switches, aliases)
  end

  def parse_global_head(input) do
    switches = default_switches()
    aliases = default_aliases()
    {options, tail, invalid} =
      OptionParser.parse_head(
        input,
        strict: switches,
        aliases: aliases,
        allow_nonexistent_atoms: true,
      )
    norm_options = normalize_options(options, switches) |> Map.new
    {norm_options, tail, invalid}
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
      aliases: aliases,
      allow_nonexistent_atoms: true,
    )
    norm_options = normalize_options(options, switches) |> Map.new
    {args, norm_options, invalid}
  end

  def default_switches() do
    [node: :atom,
     quiet: :boolean,
     dry_run: :boolean,
     vhost: :string,
     # for backwards compatibility,
     # not all commands support timeouts
     timeout: :integer,
     longnames: :boolean,
     formatter: :string,
     printer: :string,
     file: :string,
     script_name: :atom,
     rabbitmq_home: :string,
     mnesia_dir: :string,
     plugins_dir: :string,
     enabled_plugins_file: :string,
     aliases_file: :string,
     erlang_cookie: :atom
    ]
  end

  def default_aliases() do
    [p: :vhost,
     n: :node,
     q: :quiet,
     l: :longnames,
     # for backwards compatibility,
     # not all commands support timeouts
     t: :timeout]
  end

  defp build_switches(default, command) do
    command_switches = apply_if_exported(command, :switches, [], [])
    merge_if_different(default, command_switches,
                       {:command_invalid,
                        {command, {:redefining_global_switches,
                                   command_switches}}})
  end

  defp build_aliases(default, command) do
    command_aliases = apply_if_exported(command, :aliases, [], [])
    merge_if_different(default, command_aliases,
                       {:command_invalid,
                        {command, {:redefining_global_aliases,
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
      []        -> Keyword.merge(default, specific);
      conflicts ->
        # if all conflicting keys are of the same type,
        # that's acceptable
        case Enum.all?(conflicts,
              fn(c) ->
                Keyword.get(default, c) == Keyword.get(specific, c)
              end) do
          true  -> Keyword.merge(default, specific);
          false -> exit(error)
        end
    end
  end

  defp keyword_intersect(one, two) do
    one_keys = MapSet.new(Keyword.keys(one))
    two_keys = MapSet.new(Keyword.keys(two))
    intersection = MapSet.intersection(one_keys, two_keys)
    case Enum.empty?(intersection) do
      true  -> [];
      false -> MapSet.to_list(intersection)
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
