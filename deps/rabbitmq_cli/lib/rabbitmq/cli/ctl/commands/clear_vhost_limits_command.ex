## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.

defmodule RabbitMQ.CLI.Ctl.Commands.ClearVhostLimitsCommand do
  alias RabbitMQ.CLI.Core.{DocGuide, Helpers}

  @behaviour RabbitMQ.CLI.CommandBehaviour
  use RabbitMQ.CLI.DefaultOutput

  def merge_defaults(args, opts) do
    {args, Map.merge(%{vhost: "/"}, opts)}
  end

  use RabbitMQ.CLI.Core.AcceptsNoPositionalArguments

  def run([], %{node: node_name, vhost: vhost}) do
    :rabbit_misc.rpc_call(node_name, :rabbit_vhost_limit, :clear, [
      vhost,
      Helpers.cli_acting_user()
    ])
  end

  use RabbitMQ.CLI.Core.RequiresRabbitAppRunning

  def usage, do: "clear_vhost_limits [--vhost <vhost>]"

  def usage_doc_guides() do
    [
      DocGuide.virtual_hosts()
    ]
  end

  def help_section(), do: :virtual_hosts

  def description(), do: "Clears virtual host limits"

  def banner([], %{vhost: vhost}) do
    "Clearing vhost \"#{vhost}\" limits ..."
  end
end
