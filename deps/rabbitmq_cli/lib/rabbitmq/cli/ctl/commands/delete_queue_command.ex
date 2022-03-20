## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.

defmodule RabbitMQ.CLI.Ctl.Commands.DeleteQueueCommand do
  alias RabbitMQ.CLI.Core.DocGuide

  @behaviour RabbitMQ.CLI.CommandBehaviour

  def switches(), do: [if_empty: :boolean, if_unused: :boolean, timeout: :integer]
  def aliases(), do: [e: :if_empty, u: :if_unused, t: :timeout]

  def merge_defaults(args, opts) do
    {
      args,
      Map.merge(%{if_empty: false, if_unused: false, vhost: "/"}, opts)
    }
  end

  def validate([], _opts) do
    {:validation_failure, :not_enough_args}
  end

  def validate(args, _opts) when length(args) > 1 do
    {:validation_failure, :too_many_args}
  end

  def validate([""], _opts) do
    {
      :validation_failure,
      {:bad_argument, "queue name cannot be an empty string"}
    }
  end

  def validate([_], _opts) do
    :ok
  end

  ## Validate rabbit application is running
  use RabbitMQ.CLI.Core.RequiresRabbitAppRunning

  def run([qname], %{
        node: node,
        vhost: vhost,
        if_empty: if_empty,
        if_unused: if_unused,
        timeout: timeout
      }) do
    ## Generate queue resource name from queue name and vhost
    queue_resource = :rabbit_misc.r(vhost, :queue, qname)
    ## Lookup a queue on broker node using resource name
    case :rabbit_misc.rpc_call(node, :rabbit_amqqueue, :lookup, [queue_resource]) do
      {:ok, queue} ->
        ## Delete queue
        :rabbit_misc.rpc_call(
          node,
          :rabbit_amqqueue,
          :delete,
          [queue, if_unused, if_empty, "cli_user"],
          timeout
        )

      {:error, _} = error ->
        error
    end
  end

  def output({:error, :not_found}, _options) do
    {:error, RabbitMQ.CLI.Core.ExitCodes.exit_usage(), "Queue not found"}
  end

  def output({:error, :not_empty}, _options) do
    {:error, RabbitMQ.CLI.Core.ExitCodes.exit_usage(), "Queue is not empty"}
  end

  def output({:error, :in_use}, _options) do
    {:error, RabbitMQ.CLI.Core.ExitCodes.exit_usage(), "Queue is in use"}
  end

  def output({:ok, qlen}, _options) do
    {:ok, "Queue was successfully deleted with #{qlen} ready messages"}
  end

  ## Use default output for all non-special case outputs
  use RabbitMQ.CLI.DefaultOutput

  def banner([qname], %{vhost: vhost, if_empty: if_empty, if_unused: if_unused}) do
    if_empty_str =
      case if_empty do
        true -> ["if queue is empty "]
        false -> []
      end

    if_unused_str =
      case if_unused do
        true -> ["if queue is unused "]
        false -> []
      end

    "Deleting queue '#{qname}' on vhost '#{vhost}' " <>
      Enum.join(Enum.concat([if_empty_str, if_unused_str]), "and ") <> "..."
  end

  def usage(), do: "delete_queue <queue_name> [--if-empty|-e] [--if-unused|-u]"

  def usage_additional() do
    [
      ["<queue_name>", "name of the queue to delete"],
      ["--if-empty", "delete the queue if it is empty (has no messages ready for delivery)"],
      ["--if-unused", "delete the queue only if it has no consumers"]
    ]
  end

  def usage_doc_guides() do
    [
      DocGuide.queues()
    ]
  end

  def help_section(), do: :queues

  def description(), do: "Deletes a queue"
end
