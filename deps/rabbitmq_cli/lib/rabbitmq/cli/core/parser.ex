## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.

defmodule RabbitMQ.CLI.Core.Parser do
  alias RabbitMQ.CLI.{CommandBehaviour, FormatterBehaviour}
  alias RabbitMQ.CLI.Core.{CommandModules, Config, Helpers}

  def default_switches() do
    [
      node: :string,
      quiet: :boolean,
      silent: :boolean,
      dry_run: :boolean,
      vhost: :string,
      # for backwards compatibility,
      # not all commands support timeouts
      timeout: :integer,
      longnames: :boolean,
      formatter: :string,
      printer: :string,
      file: :string,
      script_name: :string,
      rabbitmq_home: :string,
      mnesia_dir: :string,
      plugins_dir: :string,
      enabled_plugins_file: :string,
      aliases_file: :string,
      erlang_cookie: :string,
      help: :boolean,
      print_stacktrace: :boolean
    ]
  end

  @atomized_options [:node, :script_name, :erlang_cookie]

  def default_aliases() do
    [
      p: :vhost,
      n: :node,
      q: :quiet,
      s: :silent,
      l: :longnames,
      # for backwards compatibility,
      # not all commands support timeouts
      t: :timeout,
      "?": :help
    ]
  end

  @spec parse([String.t()]) ::
          {command :: :no_command | atom() | {:suggest, String.t()}, command_name :: String.t(),
           arguments :: [String.t()], options :: map(),
           invalid :: [{String.t(), String.t() | nil}]}

  def parse(input) do
    {parsed_args, options, invalid} = parse_global(input)
    {command_name, command_module, arguments} = look_up_command(parsed_args, options)

    case command_module do
      nil ->
        {:no_command, command_name, arguments, options, invalid}

      {:suggest, _} = suggest ->
        {suggest, command_name, arguments, options, invalid}

      {:alias, alias_module, alias_content} ->
        {[_alias_command_name | cmd_arguments], cmd_options, cmd_invalid} =
          parse_alias(input, command_name, alias_module, alias_content, options)

        {alias_module, command_name, cmd_arguments, Helpers.atomize_values(cmd_options, @atomized_options), cmd_invalid}

      command_module when is_atom(command_module) ->
        {[^command_name | cmd_arguments], cmd_options, cmd_invalid} =
          parse_command_specific(input, command_module, options)

        {command_module, command_name, cmd_arguments, Helpers.atomize_values(cmd_options, @atomized_options), cmd_invalid}
    end
  end

  def command_suggestion(_cmd_name, empty) when empty == %{} do
    nil
  end
  def command_suggestion(typed, module_map) do
    RabbitMQ.CLI.AutoComplete.suggest_command(typed, module_map)
  end

  defp look_up_command(parsed_args, options) do
    case parsed_args do
      [cmd_name | arguments] ->
        ## This is an optimisation for pluggable command discovery.
        ## Most of the time a command will be from rabbitmqctl application
        ## so there is not point in scanning plugins for potential commands
        CommandModules.load_core(options)
        core_commands = CommandModules.module_map_core()

        command =
          case core_commands[cmd_name] do
            nil ->
              CommandModules.load(options)
              module_map = CommandModules.module_map()

              module_map[cmd_name] ||
                command_alias(cmd_name, module_map, options) ||
                command_suggestion(cmd_name, module_map)

            c ->
              c
          end

        {cmd_name, command, arguments}

      [] ->
        {"", nil, []}
    end
  end

  defp command_alias(cmd_name, module_map, options) do
    aliases = load_aliases(options)

    case aliases[cmd_name] do
      nil ->
        nil

      [alias_cmd_name | _] = alias_content ->
        case module_map[alias_cmd_name] do
          nil -> nil
          module -> {:alias, module, alias_content}
        end
    end
  end

  defp load_aliases(options) do
    aliases_file = Config.get_option(:aliases_file, options)

    case aliases_file && File.read(aliases_file) do
      ## No aliases file
      nil ->
        %{}

      {:ok, content} ->
        String.split(content, "\n")
        |> Enum.reduce(
          %{},
          fn str, acc ->
            case String.split(str, "=", parts: 2) do
              [alias_name, alias_string] ->
                Map.put(acc, String.trim(alias_name), OptionParser.split(alias_string))

              _ ->
                acc
            end
          end
        )

      {:error, err} ->
        IO.puts(:stderr, "Error reading aliases file #{aliases_file}: #{err}")
        %{}
    end
  end

  defp parse_alias(input, command_name, module, alias_content, options) do
    {pre_command_options, tail, invalid} = parse_global_head(input)
    [^command_name | other] = tail
    aliased_input = alias_content ++ other
    {args, options, command_invalid} = parse_command_specific(aliased_input, module, options)
    merged_options = Map.merge(options, pre_command_options)
    {args, merged_options, command_invalid ++ invalid}
  end

  def parse_command_specific(input, command, options \\ %{}) do
    formatter = Config.get_formatter(command, options)

    switches = build_switches(default_switches(), command, formatter)
    aliases = build_aliases(default_aliases(), command, formatter)
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
        allow_nonexistent_atoms: true
      )

    norm_options = normalize_options(options, switches) |> Map.new()
    {norm_options, tail, invalid}
  end

  def parse_global(input) do
    switches = default_switches()
    aliases = default_aliases()
    parse_generic(input, switches, aliases)
  end

  defp parse_generic(input, switches, aliases) do
    {options, args, invalid} =
      OptionParser.parse(
        input,
        strict: switches,
        aliases: aliases,
        allow_nonexistent_atoms: true
      )

    norm_options = normalize_options(options, switches) |> Map.new()
    {args, Helpers.atomize_values(norm_options, @atomized_options), invalid}
  end

  defp build_switches(default, command, formatter) do
    command_switches = CommandBehaviour.switches(command)
    formatter_switches = FormatterBehaviour.switches(formatter)

    assert_no_conflict(
      command,
      command_switches,
      formatter_switches,
      :redefining_formatter_switches
    )

    merge_if_different(
      default,
      formatter_switches,
      {:formatter_invalid,
       {formatter, {:redefining_global_switches, default, formatter_switches}}}
    )
    |> merge_if_different(
      command_switches,
      {:command_invalid, {command, {:redefining_global_switches, default, command_switches}}}
    )
  end

  defp assert_no_conflict(command, command_fields, formatter_fields, err) do
    merge_if_different(
      formatter_fields,
      command_fields,
      {:command_invalid, {command, {err, formatter_fields, command_fields}}}
    )

    :ok
  end

  defp build_aliases(default, command, formatter) do
    command_aliases = CommandBehaviour.aliases(command)
    formatter_aliases = FormatterBehaviour.aliases(formatter)

    assert_no_conflict(command, command_aliases, formatter_aliases, :redefining_formatter_aliases)

    merge_if_different(
      default,
      formatter_aliases,
      {:formatter_invalid, {command, {:redefining_global_aliases, default, formatter_aliases}}}
    )
    |> merge_if_different(
      command_aliases,
      {:command_invalid, {command, {:redefining_global_aliases, default, command_aliases}}}
    )
  end

  defp merge_if_different(default, specific, error) do
    case keyword_intersect(default, specific) do
      [] ->
        Keyword.merge(default, specific)

      conflicts ->
        # if all conflicting keys are of the same type,
        # that's acceptable
        case Enum.all?(
               conflicts,
               fn c ->
                 Keyword.get(default, c) == Keyword.get(specific, c)
               end
             ) do
          true -> Keyword.merge(default, specific)
          false -> exit(error)
        end
    end
  end

  defp keyword_intersect(one, two) do
    one_keys = MapSet.new(Keyword.keys(one))
    two_keys = MapSet.new(Keyword.keys(two))
    intersection = MapSet.intersection(one_keys, two_keys)

    case Enum.empty?(intersection) do
      true -> []
      false -> MapSet.to_list(intersection)
    end
  end

  defp normalize_options(options, switches) do
    Enum.map(
      options,
      fn {key, option} ->
        {key, normalize_type(option, switches[key])}
      end
    )
  end

  defp normalize_type(value, :atom) when is_binary(value) do
    String.to_atom(value)
  end

  defp normalize_type(value, _type) do
    value
  end
end
