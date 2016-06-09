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


defmodule RabbitMQ.CLI.Ctl.Parser do

  # Input: A list of strings
  # Output: A 2-tuple of lists: one containing the command,
  #         one containing flagged options.
  def parse(command) do
    {options, cmd, invalid} = OptionParser.parse(
      command,
      switches: build_switches([node: :atom,
                                quiet: :boolean,
                                timeout: :integer,
                                vhost: :string,
                                longnames: :boolean]),
      aliases: [p: :vhost, n: :node, q: :quiet, t: :timeout, l: :longnames]
    )
    {clear_on_empty_command(cmd), options_map(options), invalid}
  end

  defp build_switches(default) do
    Enum.reduce(RabbitMQ.CLI.Ctl.Helpers.commands,
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

  defp options_map(opts) do
    opts
    |> Map.new
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
