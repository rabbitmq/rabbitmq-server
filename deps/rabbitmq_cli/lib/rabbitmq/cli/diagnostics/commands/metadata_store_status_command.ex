## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.

defmodule RabbitMQ.CLI.Diagnostics.Commands.MetadataStoreStatusCommand do
  @behaviour RabbitMQ.CLI.CommandBehaviour
  def scopes(), do: [:diagnostics]

  def merge_defaults(args, opts), do: {args, Map.merge(%{vhost: "/"}, opts)}

  use RabbitMQ.CLI.Core.AcceptsNoPositionalArguments
  use RabbitMQ.CLI.Core.RequiresRabbitAppRunning

  def run([] = _args, %{node: node_name}) do
    case :rabbit_misc.rpc_call(node_name, :rabbit_feature_flags, :is_enabled, [:khepri_db]) do
      true ->
        :rabbit_misc.rpc_call(node_name, :rabbit_khepri, :status, [])
      false ->
         [[{<<"Metadata Store">>, "mnesia"}]]
    end
  end

  use RabbitMQ.CLI.DefaultOutput

  def formatter(), do: RabbitMQ.CLI.Formatters.PrettyTable

  def usage() do
    "metadata_store_status"
  end

  def help_section(), do: :observability_and_health_checks

  def description(), do: "Displays quorum status of Khepri metadata store"

  def banner([], %{node: node_name}),
    do: "Status of metadata store on node #{node_name} ..."
end
