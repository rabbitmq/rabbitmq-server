## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries.  All rights reserved.

defmodule RabbitMQ.CLI.Diagnostics.Commands.CheckIfMetadataStoreIsInitializedCommand do
  @moduledoc """
  Exits with a non-zero code if the node reports that its metadata store
  has finished its initialization procedure.

  This command is meant to be used in health checks.
  """

  @behaviour RabbitMQ.CLI.CommandBehaviour

  use RabbitMQ.CLI.Core.AcceptsDefaultSwitchesAndTimeout
  use RabbitMQ.CLI.Core.MergesNoDefaults
  use RabbitMQ.CLI.Core.AcceptsNoPositionalArguments

  def run([], %{node: node_name, timeout: timeout}) do
    :rabbit_misc.rpc_call(node_name, :rabbit_db, :is_init_finished, [], timeout)
  end


  def output(true, %{silent: true}) do
    {:ok, :check_passed}
  end
  def output(true, %{formatter: "json"}) do
    {:ok, %{"result" => "ok"}}
  end
  def output(true, %{node: node_name} = _options) do
    {:ok, "Metadata store on node #{node_name} has completed its initialization"}
  end

  def output(false, %{silent: true}) do
    {:error, :check_failed}
  end
  def output(false, %{node: node_name, formatter: "json"}) do
    {:error, :check_failed,
      %{
        "result"  => "error",
        "message" => "Metadata store on node #{node_name} reports to not yet have finished initialization"
      }}
  end
  def output(false, %{node: node_name} = _options) do
    {:error,
      "Metadata store on node #{node_name} reports to not yet have finished initialization"}
  end

  use RabbitMQ.CLI.DefaultOutput

  def help_section(), do: :observability_and_health_checks

  def description(),
    do:
      "Health check that exits with a non-zero code if the metadata store on target node has not yet finished initializing"

  def usage, do: "check_if_metadata_store_initialized"

  def banner([], %{node: node_name}) do
    "Checking if metadata store on node #{node_name} has finished initializing ..."
  end
end
