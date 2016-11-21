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


defmodule RabbitMQ.CLI.Core.Parser do

  # Input: A list of strings
  # Output: A 2-tuple of lists: one containing the command,
  #         one containing flagged options.
  def parse(input, switches, aliases) do
    # switches = build_switches(default_switches(), extra_switches)
    # aliases  = build_aliases(default_aliases(), extra_aliases)

    {options, cmd, invalid} = OptionParser.parse(
      input,
      strict: switches,
      aliases: aliases)
    norm_options = normalize_options(options, switches)
    {clear_on_empty_command(cmd), Map.new(norm_options), invalid}
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
     l: :longnames
    ]
  end

  def parse_global(input) do
    parse(input, default_switches(), default_aliases())
  end

  def parse_command_specific(command, input) do
    switches = build_switches(default_switches(), command)
    aliases  = build_aliases(default_aliases(), command)
    parse(input, switches, aliases)
  end

  defp build_switches(default, command) do
    command_switches = maybe_apply(command, :switches, [], [])
    merge_if_different(default, command_switches,
                       {:command_invalid,
                        {command, {:invalid_switches,
                                   command_switches}}})
  end

  defp build_aliases(default, command) do
    command_aliases = maybe_apply(command, :aliases, [], [])
    merge_if_different(default, command_aliases,
                       {:command_invalid,
                        {command, {:invalid_aliases,
                                   command_aliases}}})
  end

  defp maybe_apply(mod, fun, args, default) do
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

  # Discards entire command if first command term is empty.
  defp clear_on_empty_command(command_args) do
    case command_args do
      [] -> []
      [""|_] -> []
      [_head|_] -> command_args
    end
  end
end
