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


defmodule RabbitMQ.CLI.Ctl.Commands.QuorumStatusCommand do

  @behaviour RabbitMQ.CLI.CommandBehaviour
  use RabbitMQ.CLI.DefaultOutput

  def formatter(), do: RabbitMQ.CLI.Formatters.Table

  def scopes(), do: [:ctl, :diagnostics, :queues]

  defp default_opts() do
    %{vhost: "/"}
  end

  def merge_defaults(args, opts), do: {args, Map.merge(default_opts(), opts)}

  def validate([_|_] = args, _) when length(args) > 1 do
    {:validation_failure, :too_many_args}
  end
  def validate([_], _), do: :ok
  def validate([], _) do
    {:validation_failure, :not_enough_args}
  end

  use RabbitMQ.CLI.Core.RequiresRabbitAppRunning

  def run([name] = args, %{node: node_name, vhost: vhost}) do
    case :rabbit_misc.rpc_call(node_name, :rabbit_quorum_queue, :status, [vhost, name]) do
      {:error, :classic_queue_not_supported} ->
        {:error, "Cannot get quorum status of a classic queue"};
      other ->
        other
    end
  end

  def usage() do
    "quorum_status [-p <vhost>] <queuename>"
  end

  def banner([name], %{node: node_name}), do: "Status of quorum queue #{name} on node #{node_name} ..."
end
