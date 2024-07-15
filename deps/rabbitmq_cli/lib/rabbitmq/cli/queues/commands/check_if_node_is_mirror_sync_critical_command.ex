## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2024 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries.  All rights reserved.

defmodule RabbitMQ.CLI.Queues.Commands.CheckIfNodeIsMirrorSyncCriticalCommand do
  @moduledoc """
  DEPRECATED: this command does nothing in RabbitMQ 4.0 and newer.

  Exits with a non-zero code if there are classic mirrored queues that don't
  have any in sync mirrors online and would potentially lose data
  if the target node is shut down.

  This command is meant to be used as a pre-upgrade (pre-shutdown) check.
  """

  @behaviour RabbitMQ.CLI.CommandBehaviour

  import RabbitMQ.CLI.Core.Platform, only: [line_separator: 0]

  def scopes(), do: [:diagnostics, :queues]

  use RabbitMQ.CLI.Core.AcceptsDefaultSwitchesAndTimeout
  use RabbitMQ.CLI.Core.MergesNoDefaults
  use RabbitMQ.CLI.Core.AcceptsNoPositionalArguments
  use RabbitMQ.CLI.Core.RequiresRabbitAppRunning

  def run([], _opts) do
    :ok
  end

  def output(:ok, %{formatter: "json"}) do
    {:ok, %{"result" => "ok"}}
  end

  def output(:ok, _opts) do
    {:ok, "ok"}
  end

  use RabbitMQ.CLI.DefaultOutput

  def help_section(), do: :deprecated

  def description() do
    "DEPRECATED. Mirrored queues were removed in RabbitMQ 4.0. This command is a no-op."
  end

  def usage, do: "check_if_node_is_mirror_sync_critical"

  def banner([], %{node: node_name}) do
    "This command is DEPRECATED and is a no-op. It will be removed in a future version."
  end

end
