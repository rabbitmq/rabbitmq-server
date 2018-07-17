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
## Copyright (c) 2007-2018 Pivotal Software, Inc.  All rights reserved.


defmodule RabbitMQ.CLI.Queues.Commands.AddMember do

  alias RabbitMQ.CLI.Core.Helpers, as: Helpers
  alias RabbitMQ.CLI.Core.ExitCodes, as: ExitCodes
  import Rabbitmq.Atom.Coerce

  @behaviour RabbitMQ.CLI.CommandBehaviour

  defp default_opts, do: %{vhost: "/"}

  def merge_defaults(args, opts) do
    {args, Map.merge(default_opts(), opts)}
  end

  def validate(args, _) when length(args) < 2 do
    {:validation_failure, :not_enough_args}
  end

  def validate(args, _) when length(args) > 2 do
    {:validation_failure, :too_many_args}
  end

  def validate([_,_], _), do: :ok

  def run([name, node] = args, %{vhost: vhost, node: node_name}) do
    :rabbit_misc.rpc_call(node_name,
      :rabbit_quorum_queue,
      :add_member, [vhost, name, to_atom(node)])
  end

  use RabbitMQ.CLI.DefaultOutput

  def banner([name, node], _) do
    "Adding node #{node} to quorum queue #{name} membership..."
  end

  def usage, do: "add_member [-p <vhost>] <queuename> <node>"

  def formatter(), do: RabbitMQ.CLI.Formatters.String
end
