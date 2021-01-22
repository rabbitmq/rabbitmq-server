## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.

defmodule RabbitMQ.CLI.Ctl.Commands.ListConsumersCommand do
  alias RabbitMQ.CLI.Core.{DocGuide, Helpers}
  alias RabbitMQ.CLI.Ctl.{InfoKeys, RpcStream}

  @behaviour RabbitMQ.CLI.CommandBehaviour

  def scopes(), do: [:ctl, :diagnostics]

  use RabbitMQ.CLI.Core.AcceptsDefaultSwitchesAndTimeout

  @info_keys ~w(queue_name channel_pid consumer_tag
                ack_required prefetch_count active activity_status arguments)a

  def info_keys(), do: @info_keys

  def merge_defaults([], opts) do
    {Enum.map(@info_keys -- [:activity_status], &Atom.to_string/1),
     Map.merge(%{vhost: "/", table_headers: true}, opts)}
  end

  def merge_defaults(args, opts) do
    {args, Map.merge(%{vhost: "/", table_headers: true}, opts)}
  end

  def validate(args, _) do
    case InfoKeys.validate_info_keys(args, @info_keys) do
      {:ok, _} -> :ok
      err -> err
    end
  end

  use RabbitMQ.CLI.Core.RequiresRabbitAppRunning

  def run([_ | _] = args, %{node: node_name, timeout: timeout, vhost: vhost}) do
    info_keys = InfoKeys.prepare_info_keys(args)

    Helpers.with_nodes_in_cluster(node_name, fn nodes ->
      RpcStream.receive_list_items_with_fun(
        node_name,
        [{:rabbit_amqqueue,
        :emit_consumers_all,
        [nodes, vhost]}],
        timeout,
        info_keys,
        Kernel.length(nodes),
        fn item -> fill_consumer_active_fields(item) end
      )
    end)
  end

  use RabbitMQ.CLI.DefaultOutput

  def formatter(), do: RabbitMQ.CLI.Formatters.Table

  def usage() do
    "list_consumers [--vhost <vhost>] [--no-table-headers] [<column> ...]"
  end

  def help_section(), do: :observability_and_health_checks

  def description(), do: "Lists all consumers for a vhost"

  def usage_additional() do
    [
      ["<column>", "must be one of " <> Enum.join(Enum.sort(@info_keys), ", ")]
    ]
  end

  def usage_doc_guides() do
    [
      DocGuide.consumers()
    ]
  end

  def banner(_, %{vhost: vhost}), do: "Listing consumers in vhost #{vhost} ..."

  #
  # Implementation
  #

  # add missing fields if response comes from node < 3.8
  def fill_consumer_active_fields({[], {chunk, :continue}}) do
    {[], {chunk, :continue}}
  end

  def fill_consumer_active_fields({items, {chunk, :continue}}) do
    {Enum.map(items, fn item ->
                          case Keyword.has_key?(item, :active) do
                            true ->
                              item
                            false ->
                              Keyword.drop(item, [:arguments])
                                ++ [active: true, activity_status: :up]
                                ++ [arguments: Keyword.get(item, :arguments, [])]
                          end
                        end), {chunk, :continue}}
  end

  def fill_consumer_active_fields(v) do
    v
  end
end
