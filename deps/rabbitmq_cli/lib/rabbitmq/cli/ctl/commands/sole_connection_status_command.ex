## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries.  All rights reserved.

defmodule RabbitMQ.CLI.Ctl.Commands.SoleConnectionStatusCommand do
  @behaviour RabbitMQ.CLI.CommandBehaviour
  def scopes(), do: [:ctl, :diagnostics]

  def merge_defaults(args, opts), do: {args, opts}

  use RabbitMQ.CLI.Core.AcceptsNoPositionalArguments
  use RabbitMQ.CLI.Core.RequiresRabbitAppRunning

  def run([] = _args, %{node: node_name}) do
    :rabbit_misc.rpc_call(node_name, :rabbit_sole_conn, :status, [])
  end

  def output({:error, :sole_conn_not_started_or_available}, _opts) do
    {:error, RabbitMQ.CLI.Core.ExitCodes.exit_software(),
     "Cannot get AMQP 1.0 sole connection enforcement status as it is not started or unavailable"}
  end

  def output({:error, {:feature_flag_disabled, feature_flag_name}}, _opts) do
    {:error, RabbitMQ.CLI.Core.ExitCodes.exit_software(),
     "AMQP 1.0 sole connection enforcement requires the '#{feature_flag_name}' feature flag to be enabled cluster-wide"}
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
