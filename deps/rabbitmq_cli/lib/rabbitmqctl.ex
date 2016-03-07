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


defmodule RabbitMQCtl do
  import Parser
  import Helpers

  def main(command) do
    :net_kernel.start([:rabbitmqctl, :shortnames])

    {parsed_cmd, options} = parse(command)

    case options[:node] do
      nil -> connect_to_rabbitmq |> IO.puts
      _   -> options[:node] |> String.to_atom |> connect_to_rabbitmq |> IO.puts
    end

    case Helpers.is_command? parsed_cmd do
      false -> HelpCommand.help
      true  -> run_command(parsed_cmd, options)
    end

    :net_kernel.stop()
  end

  defp print_nodedown_error(options) do
    target_node = options[:node] || get_rabbit_hostname

    IO.puts "Status of #{target_node} ..."
    IO.puts "Error: unable to connect to node '#{target_node}': nodedown"
  end

  defp run_command([], _), do: HelpCommand.help
  defp run_command([cmd | arguments], options) do
    {result, _} = Code.eval_string(
      "#{command_string(cmd)}(args, opts)",
      [args: arguments, opts: options]
    )

    case result do
      {:badrpc, :nodedown}  -> print_nodedown_error(options)
      _                     -> IO.inspect result
    end
  end

  defp command_string(cmd_name) do
    "#{Helpers.commands[cmd_name]}.#{cmd_name}"
  end
end
