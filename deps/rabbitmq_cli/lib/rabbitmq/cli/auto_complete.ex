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


defmodule Rabbitmq.CLI.AutoComplete do
  alias RabbitMQ.CLI.Core.Parser, as: Parser
  alias RabbitMQ.CLI.Core.CommandModules, as: CommandModules

  @spec complete(String.t) :: {:ok, [String.t]}
  def complete(str) do
    tokens = String.split(str, " ", trim: true)
    case List.last(tokens) do
      nil        -> [];
      last_token ->
        {args, opts, _} = Parser.parse(tokens)
        CommandModules.load(opts)
        variants = case args do
          []      -> complete_default_opts(last_token);
          [cmd|_] -> complete_command_opts(cmd, last_token)
        end
        variants
        # last_token_position = length(tokens) - 1
        # Enum.map(variants,
        #          fn(v) ->
        #            tokens
        #            |> List.replace_at(last_token_position, v)
        #            |> Enum.join(" ")
        #          end)
    end
  end

  def complete_default_opts(opt) do
    Parser.default_switches
    |> Keyword.keys
    |> Enum.map(fn(sw) -> "--" <> to_string(sw) end)
    |> select_starts_with(opt)
  end

  def complete_command_opts(cmd, opt) do
    commands = CommandModules.module_map
    case commands[cmd] do
      nil     ->
        commands
        |> Map.keys
        |> select_starts_with(opt)
      command ->
        complete_opts(command, opt)
    end
  end

  def complete_opts(command, <<"-", _ :: binary>> = opt) do
    switches = command.switches
               |> Keyword.keys
               |> Enum.map(fn(sw) -> "--" <> to_string(sw) end)

    # aliases = command.aliases
    #           |> Keyword.keys
    #           |> Enum.map(fn(al) -> "-" <> to_string(al) end)
    select_starts_with(switches, opt)
  end
  def complete_opts(_, _) do
    []
  end

  defp select_starts_with(list, prefix) do
    Enum.filter(list, fn(el) -> String.starts_with?(el, prefix) end)
  end
end
