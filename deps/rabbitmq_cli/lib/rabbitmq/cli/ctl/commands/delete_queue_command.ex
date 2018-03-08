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


defmodule RabbitMQ.CLI.Ctl.Commands.DeleteQueueCommand do
  @behaviour RabbitMQ.CLI.CommandBehaviour

  def switches(), do: [if_empty: :boolean, if_unused: :boolean, timeout: :integer]
  def aliases(), do: [e: :if_empty, u: :is_unused, t: :timeout]

  def usage(), do: "delete_queue queue_name [--if_empty|-e] [--if_unused|-u]"

  def banner([qname], %{vhost: vhost,
                        if_empty: if_empty,
                        if_unused: if_unused}) do
    if_empty_str = case if_empty do
      true  -> ["if queue is empty "]
      false -> []
    end
    if_unused_str = case if_unused do
      true  -> ["if queue is unused "]
      false -> []
    end
    "Deleting queue '#{qname}' on vhost '#{vhost}' " <>
      Enum.join(Enum.concat([if_empty_str, if_unused_str]), "and ") <> "..."
  end

  def merge_defaults(args, options) do
    {
      args,
      Map.merge(%{if_empty: false, if_unused: false, vhost: "/"}, options)
    }
  end

  ## Validate rabbit application is running
  use RabbitMQ.CLI.Core.RequiresRabbitAppRunning

  def validate([], _options) do
    {:validation_failure, :not_enough_args}
  end
  def validate([_,_|_], _options) do
    {:validation_failure, :too_many_args}
  end
  def validate([""], _options) do
    {
      :validation_failure,
      {:bad_argument, "queue name cannot be empty string."}
    }
  end
  def validate([_], _options) do
    :ok
  end

  def run([qname], %{node: node, vhost: vhost,
                     if_empty: if_empty, if_unused: if_unused,
                     timeout: timeout}) do
    ## Generate queue resource name from queue name and vhost
    queue_resource = :rabbit_misc.r(vhost, :queue, qname)
    ## Lookup a queue on broker node using resource name
    case :rabbit_misc.rpc_call(node, :rabbit_amqqueue, :lookup,
                                     [queue_resource]) do
      {:ok, queue} ->
        ## Delete queue
        :rabbit_misc.rpc_call(node, :rabbit_amqqueue, :delete,
                                    [queue, if_unused, if_empty, "cli_user"],
                              timeout);
      {:error, _} = error -> error
    end
  end

  def output({:error, :not_found}, _options) do
    {:error, RabbitMQ.CLI.Core.ExitCodes.exit_usage, "Queue not found"}
  end
  def output({:error, :not_empty}, _options) do
    {:error, RabbitMQ.CLI.Core.ExitCodes.exit_usage, "Queue is not empty"}
  end
  def output({:error, :in_use}, _options) do
    {:error, RabbitMQ.CLI.Core.ExitCodes.exit_usage, "Queue is in use"}
  end
  def output({:ok, qlen}, _options) do
    {:ok, "Queue was successfully deleted with #{qlen} messages"}
  end
  ## Use default output for all non-special case outputs
  use RabbitMQ.CLI.DefaultOutput
end
