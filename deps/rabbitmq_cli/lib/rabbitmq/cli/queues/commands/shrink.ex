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
## Copyright (c) 2007-2019 Pivotal Software, Inc.  All rights reserved.

defmodule RabbitMQ.CLI.Queues.Commands.Shrink do
  import Rabbitmq.Atom.Coerce

  @behaviour RabbitMQ.CLI.CommandBehaviour

  defp default_opts, do: %{}

  def merge_defaults(args, opts) do
    {args, Map.merge(default_opts(), opts)}
  end

  def validate(args, _) when length(args) < 1 do
    {:validation_failure, :not_enough_args}
  end

  def validate(args, _) when length(args) > 1 do
    {:validation_failure, :too_many_args}
  end

  def validate([_], _), do: :ok

  use RabbitMQ.CLI.Core.RequiresRabbitAppRunning

  def run([node], %{node: node_name}) do
    case :rabbit_misc.rpc_call(node_name, :rabbit_quorum_queue, :shrink_all, [
           to_atom(node)
         ]) do
      results ->
        results
    end
  end

  use RabbitMQ.CLI.DefaultOutput

  def formatter(), do: RabbitMQ.CLI.Formatters.Table

  def banner([node], _) do
    "Shrinking queues on #{node}..."
  end

  def usage, do: "shrink <node>"
end
