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


defmodule RabbitMQ.CLI.Ctl.CommandModules do
  def module_map do
    case Application.get_env(:rabbitmqctl, :commands) do
      nil -> load;
      val -> val
    end
  end

  def load do
    scope = script_scope
    commands = load_commands(scope)
    Application.put_env(:rabbitmqctl, :commands, commands)
    commands
  end

  def script_scope do
    scopes = Application.get_env(:rabbitmqctl, :scopes, [])
    scopes[script_name] || :all
  end

  def script_name do
    Path.basename(:escript.script_name()) |> String.to_atom
  end

  defp load_commands(scope) do
    modules = loadable_modules()
    modules
    |> Enum.filter(fn(path) ->
                     to_string(path) =~ ~r/RabbitMQ.CLI.*.Commands/
                   end)
    |> Enum.map(fn(path) ->
                  Path.rootname(path, '.beam')
                  |> String.to_atom
                  |> Code.ensure_loaded
                end)
    |> Enum.filter_map(fn({res, _}) -> res == :module end,
                       fn({_, mod}) -> command_tuple(mod) end)
    |> Enum.filter(fn({_, cmd}) -> command_in_scope(cmd, scope) end)
    |> Map.new
  end

  defp loadable_modules do
    :code.get_path()
    |> Enum.flat_map(fn(path) ->
         {:ok, modules} = :erl_prim_loader.list_dir(path)
         modules
       end)
  end

  defp command_tuple(cmd) do
    {
      cmd
      |> to_string
      |> strip_namespace
      |> to_snake_case
      |> String.replace_suffix("_command", ""),
      cmd
    }
  end

  def strip_namespace(str) do
    str
    |> String.split(".")
    |> List.last
  end

  def to_snake_case(<<c, str :: binary>>) do
    tail = for <<c <- str>>, into: "", do: snake(c)
    <<to_lower_char(c), tail :: binary >>
  end

  defp snake(c) do
    if (c >= ?A) and (c <= ?Z) do
      <<"_", c+32>>
    else
      <<c>>
    end
  end

  defp to_lower_char(c) do
    if (c >= ?A) and (c <= ?Z) do
      c+32
    else
      c
    end
  end

  defp command_in_scope(_cmd, :all) do
    true
  end
  defp command_in_scope(cmd, scope) do
    Code.ensure_loaded(cmd)
    if :erlang.function_exported(cmd, :scopes, 0) do
      Enum.member?(cmd.scopes(), scope)
    end
  end
end
