## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2023 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries.  All rights reserved.

defmodule RabbitMQ.CLI.Ctl.Commands.NodeHealthCheckCommand do
  alias RabbitMQ.CLI.Core.DocGuide

  @behaviour RabbitMQ.CLI.CommandBehaviour

  def scopes(), do: [:ctl, :diagnostics]
  use RabbitMQ.CLI.Core.AcceptsDefaultSwitchesAndTimeout

  def merge_defaults(args, opts) do
    {args, opts}
  end

  use RabbitMQ.CLI.Core.AcceptsNoPositionalArguments
  use RabbitMQ.CLI.Core.RequiresRabbitAppRunning

  def run([], _opts) do
    :ok
  end

  use RabbitMQ.CLI.DefaultOutput

  def usage, do: "node_health_check"

  def usage_doc_guides() do
    [
      DocGuide.monitoring()
    ]
  end

  def help_section(), do: :deprecated

  def description() do
    "DEPRECATED. This command is a no-op. " <>
      "See https://www.rabbitmq.com/monitoring.html#health-checks"
  end

  def banner(_, _opts) do
    [
      "This command is DEPRECATED and is a no-op. It will be removed in a future version. ",
      "Use one of the options covered in https://www.rabbitmq.com/monitoring.html#health-checks instead."
    ]
  end
end
