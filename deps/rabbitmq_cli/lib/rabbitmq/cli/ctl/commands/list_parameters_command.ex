## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.

defmodule RabbitMQ.CLI.Ctl.Commands.ListParametersCommand do
  alias RabbitMQ.CLI.Core.DocGuide

  @behaviour RabbitMQ.CLI.CommandBehaviour

  def scopes(), do: [:ctl, :diagnostics]
  use RabbitMQ.CLI.Core.AcceptsDefaultSwitchesAndTimeout

  def merge_defaults(args, opts) do
    {args, Map.merge(%{vhost: "/", table_headers: true}, opts)}
  end

  use RabbitMQ.CLI.Core.AcceptsNoPositionalArguments
  use RabbitMQ.CLI.Core.RequiresRabbitAppRunning

  def run([], %{node: node_name, timeout: timeout, vhost: vhost}) do
    :rabbit_misc.rpc_call(
      node_name,
      :rabbit_runtime_parameters,
      :list_formatted,
      [vhost],
      timeout
    )
  end

  use RabbitMQ.CLI.DefaultOutput

  def formatter(), do: RabbitMQ.CLI.Formatters.Table

  def usage, do: "list_parameters [--vhost <vhost>] [--no-table-headers]"

  def usage_doc_guides() do
    [
      DocGuide.parameters()
    ]
  end

  def help_section(), do: :parameters

  def description(), do: "Lists runtime parameters for a virtual host"

  def banner(_, %{vhost: vhost}), do: "Listing runtime parameters for vhost \"#{vhost}\" ..."
end
