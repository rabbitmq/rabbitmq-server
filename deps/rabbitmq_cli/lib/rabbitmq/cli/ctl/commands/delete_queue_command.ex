## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries.  All rights reserved.

defmodule RabbitMQ.CLI.Ctl.Commands.DeleteQueueCommand do
  alias RabbitMQ.CLI.Core.{DocGuide, Users}

  @behaviour RabbitMQ.CLI.CommandBehaviour

  def switches(), do: [if_empty: :boolean, if_unused: :boolean, force: :boolean, timeout: :integer]
  def aliases(), do: [e: :if_empty, u: :if_unused, t: :timeout]

  def merge_defaults(args, opts) do
    {
      args,
      Map.merge(%{if_empty: false, if_unused: false, force: false, vhost: "/"}, opts)
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
        force: force,
        timeout: timeout
      }) do
    ## Generate queue resource name from queue name and vhost
    queue_resource = :rabbit_misc.r(vhost, :queue, qname)
    user = if force, do: Users.internal_user, else: Users.cli_user
    ## Lookup a queue on broker node using resource name
    case :rabbit_misc.rpc_call(node, :rabbit_amqqueue, :lookup, [queue_resource]) do
      {:ok, queue} ->
        ## Delete queue
        case :rabbit_misc.rpc_call(node,
              :rabbit_amqqueue,
              :delete_with,
              [queue, if_unused, if_empty, user],
              timeout
            ) do
          {:ok, _} = ok -> ok

          {:badrpc, {:EXIT, {:amqp_error, :resource_locked, _, :none}}} ->
              {:error, :protected}

          other_error -> other_error
        end

      {:error, _} = error ->
        error
    end
  end

  def output({:error, :protected}, _options) do
    {:error, RabbitMQ.CLI.Core.ExitCodes.exit_usage(), "The queue is locked or protected from deletion"}
  end

  def output({:error, :not_found}, _options) do
    {:error, RabbitMQ.CLI.Core.ExitCodes.exit_usage(), "No such queue was found"}
  end

  def output({:error, :not_empty}, _options) do
    {:error, RabbitMQ.CLI.Core.ExitCodes.exit_usage(), "The queue is not empty"}
  end

  def output({:error, :in_use}, _options) do
    {:error, RabbitMQ.CLI.Core.ExitCodes.exit_usage(), "The queue is in use"}
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

  def usage(), do: "delete_queue [--vhost <vhost>] <queue_name> [--if-empty|-e] [--if-unused|-u] [--force]"

  def usage_additional() do
    [
      ["--vhost", "Virtual host name"],
      ["<queue_name>", "name of the queue to delete"],
      ["--if-empty", "delete the queue if it is empty (has no messages ready for delivery)"],
      ["--if-unused", "delete the queue only if it has no consumers"],
      ["--force", "delete the queue even if it is protected"]
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
