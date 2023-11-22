## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2023 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries.  All rights reserved.

defmodule RabbitMQ.CLI.Diagnostics.Commands.ListPoliciesThatMatchCommand do
  alias RabbitMQ.CLI.Core.DocGuide

  @behaviour RabbitMQ.CLI.CommandBehaviour
  def scopes(), do: [:diagnostics]

  def switches(), do: [object_type: :string]
  use RabbitMQ.CLI.Core.AcceptsOnePositionalArgument

  def merge_defaults(args, opts) do
    {args, Map.merge(%{vhost: "/", object_type: "queue"}, opts)}
  end

  use RabbitMQ.CLI.Core.RequiresRabbitAppRunning

  def run([queue], %{node: node_name, vhost: vhost, object_type: object_type, timeout: timeout}) do
    resource =
      :rabbit_misc.rpc_call(
        node_name,
        :rabbit_misc,
        :r,
        [vhost, String.to_atom(object_type), queue],
        timeout
      )

    res =
      case object_type do
        "exchange" ->
          resource

        "queue" ->
          :rabbit_misc.rpc_call(
            node_name,
            :rabbit_amqqueue,
            :lookup,
            [resource],
            timeout
          )
      end

    case res do
      {:ok, q} ->
        list_policies_that_match(node_name, q, timeout)

      {:resource, _, :exchange, _} = ex ->
        list_policies_that_match(node_name, ex, timeout)

      _ ->
        res
    end
  end

  def output([], %{node: node_name, formatter: "json"}) do
    {:ok, %{"result" => "ok", "policies" => []}}
  end

  def output({:error, :not_found}, %{node: node_name, formatter: "json"}) do
    {:ok,
     %{"result" => "error", "message" => "object (queue, exchange) not found", "policies" => []}}
  end

  def output(value, %{node: node_name, formatter: "json"}) when is_list(value) do
    {:ok, %{"result" => "ok", "policies" => value}}
  end

  use RabbitMQ.CLI.DefaultOutput

  def formatter(), do: RabbitMQ.CLI.Formatters.PrettyTable

  def usage, do: "list_policies_that_match [--object-type <type>] <name>"

  def usage_additional() do
    [
      ["<name>", "The name of the queue/exchange"],
      ["--object-type <queue | exchange>", "the type of object to match (default: queue)"]
    ]
  end

  def usage_doc_guides() do
    [
      DocGuide.queues()
    ]
  end

  def help_section(), do: :policies

  def description(),
    do:
      "Lists all policies matching a queue/exchange (only the highest priority policy is active)"

  def banner([name], %{vhost: vhost, object_type: object_type}) do
    "Listing policies that match #{object_type} '#{name}' in vhost '#{vhost}' ..."
  end

  #
  # Implementation
  #

  defp list_policies_that_match(node_name, name, timeout) do
    res = :rabbit_misc.rpc_call(node_name, :rabbit_policy, :match_all, [name], timeout)

    case res do
      {:ok, _message_count} -> :ok
      _ -> res
    end
  end
end
