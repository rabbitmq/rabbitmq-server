## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries.  All rights reserved.

defmodule RabbitMQ.CLI.Ctl.Commands.AmqpSoleConnStatusCommand do
  @behaviour RabbitMQ.CLI.CommandBehaviour
  def scopes(), do: [:ctl, :diagnostics]

  def merge_defaults(args, opts), do: {args, opts}

  use RabbitMQ.CLI.Core.AcceptsNoPositionalArguments
  use RabbitMQ.CLI.Core.RequiresRabbitAppRunning

  def run([] = _args, %{node: node_name}) do
    case :rabbit_misc.rpc_call(node_name, :rabbit_amqp_sole_conn, :status, []) do
      {:error, :sole_conn_not_started_or_available} ->
        {:error,
         "Cannot get AMQP 1.0 sole connection enforcement status as it is not started or unavailable"}

      {:error, :sole_conn_feature_flag_not_enabled} ->
        {:error,
         "AMQP 1.0 sole connection enforcement requires all nodes in the cluster " <>
           "to run RabbitMQ 4.4.0 or later"}

      other ->
        other
    end
  end

  use RabbitMQ.CLI.DefaultOutput

  def formatter(), do: RabbitMQ.CLI.Formatters.PrettyTable

  def usage() do
    "amqp_sole_conn_status"
  end

  def usage_additional do
    []
  end

  def usage_doc_guides() do
    []
  end

  def help_section(), do: :observability_and_health_checks

  def description(),
    do: "Displays raft status of the AMQP 1.0 sole connection enforcement feature"

  def banner([], %{node: _node_name}),
    do: "Status of AMQP 1.0 sole connection enforcement ..."
end
