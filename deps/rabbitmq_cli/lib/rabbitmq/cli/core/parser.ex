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
  def parse(command) do
    switches = build_switches(default_switches())
    {options, cmd, invalid} = OptionParser.parse(
      command,
      strict: switches,
      aliases: build_aliases([p: :vhost, n: :node, q: :quiet,
                              t: :timeout, l: :longnames])
    )
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

  defp build_switches(default) do
    Enum.reduce(RabbitMQ.CLI.Core.CommandModules.module_map,
                default,
                fn({_, _}, {:error, _} = err) -> err;
                  ({_, command}, switches) ->
                    command_switches = command.switches()
                    case Enum.filter(command_switches,
                                     fn({key, val}) ->
                                       existing_val = switches[key]
                                       existing_val != nil and existing_val != val
                                     end) do
                      [] -> switches ++ command_switches;
                      _  -> exit({:command_invalid,
                                  {command, {:invalid_switches,
                                             command_switches}}})
                    end
                end)
  end

  defp build_aliases(default) do
    Enum.reduce(RabbitMQ.CLI.Core.CommandModules.module_map,
                default,
                fn({_, _}, {:error, _} = err) -> err;
                  ({_, command}, aliases) ->
                    command_aliases = command.aliases()
                    case Enum.filter(command_aliases,
                                     fn({key, val}) ->
                                       existing_val = aliases[key]
                                       existing_val != nil and existing_val != val
                                     end) do
                      [] -> aliases ++ command_aliases;
                      _  -> exit({:command_invalid,
                                  {command, {:invalid_switches,
                                             command_aliases}}})
                    end
                end)
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
