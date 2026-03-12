## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries.  All rights reserved.

defmodule RabbitMQ.CLI.Diagnostics.Commands.CheckLocalAlarmsCommand do
  @moduledoc """
  DEPRECATED. This command is a no-op.

  Local alarms (e.g. file descriptor limits) have been removed.
  All alarms are now cluster-wide resource alarms.
  """

  alias RabbitMQ.CLI.Core.DocGuide

  @behaviour RabbitMQ.CLI.CommandBehaviour

  use RabbitMQ.CLI.Core.AcceptsDefaultSwitchesAndTimeout
  use RabbitMQ.CLI.Core.MergesNoDefaults
  use RabbitMQ.CLI.Core.AcceptsNoPositionalArguments
  use RabbitMQ.CLI.Core.RequiresRabbitAppRunning

  def run([], _opts) do
    []
  end

  def output([], %{formatter: "json"}) do
    {:ok, %{"result" => "ok"}}
  end

  def output([], %{silent: true}) do
    {:ok, :check_passed}
  end

  def output([], %{node: node_name}) do
    {:ok, "Node #{node_name} reported no local alarms"}
  end

  use RabbitMQ.CLI.DefaultOutput

  def help_section(), do: :deprecated

  def description() do
    "DEPRECATED. This command is a no-op. Local alarms have been removed. " <>
      "See https://www.rabbitmq.com/alarms.html"
  end

  def usage, do: "check_local_alarms"

  def usage_doc_guides() do
    [
      DocGuide.alarms()
    ]
  end

  def banner([], _opts) do
    ["This command is DEPRECATED and is a no-op. It will be removed in a future version."]
  end
end
