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


defmodule Parser do

  # Input: A list of strings
  # Output: A 2-tuple of lists: one containing the command,
  #         one containing flagged options.
  def parse(command) do
    {options, cmd, invalid} = OptionParser.parse(
      command,
      switches: [node: :atom, quiet: :boolean, timeout: :integer,
                 online: :boolean, offline: :boolean],
      aliases: [p: :param, n: :node, q: :quiet, t: :timeout]
    )
    {clear_on_empty_command(cmd), options_map(options, invalid)}
  end

  defp options_map(opts, invalid) do
    opts ++ invalid
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
