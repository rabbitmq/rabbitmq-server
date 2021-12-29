## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.

defmodule RabbitMQ.CLI.Ctl.Commands.ListQueuesCommand do
  require RabbitMQ.CLI.Ctl.InfoKeys
  require RabbitMQ.CLI.Ctl.RpcStream

  alias RabbitMQ.CLI.Ctl.{InfoKeys, RpcStream}
  alias RabbitMQ.CLI.Core.{DocGuide, Helpers}

  @behaviour RabbitMQ.CLI.CommandBehaviour

  @default_timeout 60_000
  @info_keys ~w(name durable auto_delete
            arguments policy pid owner_pid exclusive exclusive_consumer_pid
            exclusive_consumer_tag messages_ready messages_unacknowledged messages
            messages_ready_ram messages_unacknowledged_ram messages_ram
            messages_persistent message_bytes message_bytes_ready
            message_bytes_unacknowledged message_bytes_ram message_bytes_persistent
            head_message_timestamp disk_reads disk_writes consumers
            # these are aliases
            consumer_utilisation consumer_capacity
            memory slave_pids synchronised_slave_pids state type
            leader members online confirm_on)a

  def description(), do: "Lists queues and their properties"
  def usage(), do: "list_queues [--vhost <vhost>] [--online] [--offline] [--local] [--no-table-headers] [<column>, ...]"
  def scopes(), do: [:ctl, :diagnostics]
  def switches(), do: [offline: :boolean,
                       online: :boolean,
                       local: :boolean,
                       timeout: :integer]
  def aliases(), do: [t: :timeout]

  def info_keys(), do: @info_keys

  defp default_opts() do
    %{vhost: "/", offline: false, online: false, local: false, table_headers: true}
  end

  def merge_defaults([_ | _] = args, opts) do
    timeout =
      case opts[:timeout] do
        nil -> @default_timeout
        :infinity -> @default_timeout
        other -> other
      end

    {args,
     Map.merge(
       default_opts(),
       Map.merge(opts, %{timeout: timeout})
     )}
  end

  def merge_defaults([], opts) do
    merge_defaults(~w(name messages), opts)
  end

  def validate(args, _opts) do
    case InfoKeys.validate_info_keys(args, @info_keys) do
      {:ok, _} -> :ok
      err -> err
    end
  end

  # note that --offline for this command has a different meaning:
  # it lists queues with unavailable masters
  use RabbitMQ.CLI.Core.RequiresRabbitAppRunning

  def run([_ | _] = args, %{
        node: node_name,
        timeout: timeout,
        vhost: vhost,
        online: online_opt,
        offline: offline_opt,
        local: local_opt
      }) do
    {online, offline} =
      case {online_opt, offline_opt} do
        {false, false} -> {true, true}
        other -> other
      end

    info_keys = InfoKeys.prepare_info_keys(args)

    Helpers.with_nodes_in_cluster(node_name, fn nodes ->
      offline_mfa = {:rabbit_amqqueue, :emit_info_down, [vhost, info_keys]}
      local_mfa = {:rabbit_amqqueue, :emit_info_local, [vhost, info_keys]}
      online_mfa = {:rabbit_amqqueue, :emit_info_all, [nodes, vhost, info_keys]}

      {chunks, mfas} =
        case {local_opt, offline, online} do
          # Local takes precedence
          {true, _, _} -> {1, [local_mfa]}
          {_, true, true} -> {Kernel.length(nodes) + 1, [offline_mfa, online_mfa]}
          {_, false, true} -> {Kernel.length(nodes), [online_mfa]}
          {_, true, false} -> {1, [offline_mfa]}
        end

      RpcStream.receive_list_items_with_fun(node_name, mfas, timeout, info_keys, chunks, fn
        {{:error, {:badrpc, {:timeout, to}}}, :finished} ->
          {{:error,
            {:badrpc,
             {:timeout, to,
              "Some queue(s) are unresponsive, use list_unresponsive_queues command."}}},
           :finished}

        any ->
          any
      end)
    end)
  end

  use RabbitMQ.CLI.DefaultOutput

  def formatter(), do: RabbitMQ.CLI.Formatters.Table

  def help_section(), do: :observability_and_health_checks

  def usage_additional do
    [
      ["<column>", "must be one of " <> Enum.join(Enum.sort(@info_keys), ", ")],
      ["--online", "lists only queues on online (reachable) nodes"],
      ["--offline", "lists only queues on offline (unreachable) nodes"],
      ["--local", "only return queues hosted on the target node"]
    ]
  end

  def usage_doc_guides() do
    [
      DocGuide.queues()
    ]
  end

  def banner(_, %{vhost: vhost, timeout: timeout}) do
    [
      "Timeout: #{timeout / 1000} seconds ...",
      "Listing queues for vhost #{vhost} ..."
    ]
  end
end
