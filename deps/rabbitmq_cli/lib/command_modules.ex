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



defmodule CommandModules do
  def generate_module_map do
    Mix.Project.config[:elixirc_paths]
    |> Mix.Utils.extract_files("*_command.ex")
    |> Enum.map(fn(filename) -> filename |> command_name |> command_tuple end)
    |> Map.new
  end

  # Takes a fully-pathed snake_case filename and returns the base
  # command from it (e.g., "/path/to/status_command" becomes "status")
  defp command_name(file_name) do
    file_name
    |> Path.basename
    |> Path.rootname(".ex")
    |> String.replace_suffix("_command", "")
  end

  # Takes a name (e.g., "status_command") and returns a {atom, string}
  # tuple (e.g., {:status, "StatusCommand"}) that we can use to 
  # generate our map
  defp command_tuple(cmd_name) do
    {
      cmd_name,
      Macro.camelize(cmd_name) <> "Command"
    }
  end
end
