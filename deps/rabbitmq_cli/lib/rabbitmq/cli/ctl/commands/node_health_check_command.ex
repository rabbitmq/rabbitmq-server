## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.

defmodule RabbitMQ.CLI.Ctl.Commands.NodeHealthCheckCommand do
  alias RabbitMQ.CLI.Core.DocGuide

  @behaviour RabbitMQ.CLI.CommandBehaviour

  @default_timeout 70_000

  def scopes(), do: [:ctl, :diagnostics]
  use RabbitMQ.CLI.Core.AcceptsDefaultSwitchesAndTimeout

  def merge_defaults(args, opts) do
    timeout =
      case opts[:timeout] do
        nil -> @default_timeout
        :infinity -> @default_timeout
        other -> other
      end

    {args, Map.merge(opts, %{timeout: timeout})}
  end

  use RabbitMQ.CLI.Core.AcceptsNoPositionalArguments

  def run([], _opts) do
    :ok
  end

  def output(:ok, _) do
    {:ok, "This command is a no-op. See https://www.rabbitmq.com/monitoring.html#health-checks for modern alternatives"}
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
    "DEPRECATED. Does not perform any checks (is a no-op). " <>
    "See https://www.rabbitmq.com/monitoring.html#health-checks for modern alternatives"
  end

  def banner(_, _) do
    [
      "This command has been DEPRECATED since 2019 and no longer has any effect.",
      "Use one of the options covered in https://www.rabbitmq.com/monitoring.html#health-checks instead."
    ]
  end
end
