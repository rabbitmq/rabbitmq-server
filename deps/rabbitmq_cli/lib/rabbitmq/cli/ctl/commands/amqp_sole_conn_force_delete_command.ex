## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries.  All rights reserved.

defmodule RabbitMQ.CLI.Ctl.Commands.AmqpSoleConnForceDeleteCommand do
  alias RabbitMQ.CLI.Core.DocGuide

  @behaviour RabbitMQ.CLI.CommandBehaviour

  def merge_defaults(args, opts), do: {args, Map.merge(%{vhost: "/"}, opts)}

  use RabbitMQ.CLI.Core.AcceptsOnePositionalArgument
  use RabbitMQ.CLI.Core.RequiresRabbitAppRunning

  def run([container_id], %{node: node_name, vhost: vhost}) do
    :rabbit_misc.rpc_call(node_name, :rabbit_amqp_sole_conn, :force_delete, [
      vhost,
      container_id
    ])
  end

  def output({:error, :not_found}, _opts) do
    {:error, RabbitMQ.CLI.Core.ExitCodes.exit_usage(),
     "Could not find an AMQP 1.0 sole connection lease for the given virtual host and container ID"}
  end

  def output({:error, :sole_conn_not_started_or_available}, _opts) do
    {:error, RabbitMQ.CLI.Core.ExitCodes.exit_software(),
     "Cannot force-delete the AMQP 1.0 sole connection lease as the enforcement feature is not started or unavailable"}
  end

  def output({:error, :sole_conn_feature_flag_not_enabled}, _opts) do
    {:error, RabbitMQ.CLI.Core.ExitCodes.exit_software(),
     "AMQP 1.0 sole connection enforcement requires all nodes in the cluster to run RabbitMQ 4.4.0 or later"}
  end

  use RabbitMQ.CLI.DefaultOutput

  def usage() do
    "amqp_sole_conn_force_delete [--vhost <vhost>] <container_id>"
  end

  def usage_additional do
    [
      ["<container_id>", "AMQP 1.0 container ID of the connection lease to force-delete"]
    ]
  end

  def usage_doc_guides() do
    [
      DocGuide.connections()
    ]
  end

  def help_section(), do: :operations

  def description(),
    do:
      "Force-deletes an AMQP 1.0 sole connection enforcement lease, useful when it was not cleaned up automatically"

  def banner([container_id], %{vhost: vhost}),
    do:
      "Force-deleting AMQP 1.0 sole connection lease for container ID \"#{container_id}\" on vhost \"#{vhost}\" ..."
end
