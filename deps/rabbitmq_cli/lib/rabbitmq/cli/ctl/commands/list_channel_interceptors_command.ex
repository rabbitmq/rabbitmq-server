## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.

defmodule RabbitMQ.CLI.Ctl.Commands.ListChannelInterceptorsCommand do
  @behaviour RabbitMQ.CLI.CommandBehaviour

  def scopes(), do: [:ctl, :diagnostics]

  use RabbitMQ.CLI.Core.AcceptsDefaultSwitchesAndTimeout
  use RabbitMQ.CLI.Core.AcceptsNoPositionalArguments
  use RabbitMQ.CLI.Core.RequiresRabbitAppRunning

  def merge_defaults(args, opts) do
    {args, Map.merge(%{table_headers: true}, opts)}
  end

  def run([], %{node: node_name, timeout: timeout}) do
    case :rabbit_misc.rpc_call(node_name, :rabbit_channel_interceptor, :list, [], timeout) do
      {:badrpc, _} = err ->
        err

      interceptors ->
        Enum.map(interceptors, fn info ->
          Keyword.update!(info, :applies_to, &Enum.join(&1, ", "))
        end)
    end
  end

  use RabbitMQ.CLI.DefaultOutput

  def formatter(), do: RabbitMQ.CLI.Formatters.Table

  def usage, do: "list_channel_interceptors [--no-table-headers]"

  def help_section(), do: :observability_and_health_checks

  def description(),
    do: "Lists registered channel interceptors with their name, intercepted AMQP operations, and priority"

  def banner(_, _), do: "Listing channel interceptors ..."
end
