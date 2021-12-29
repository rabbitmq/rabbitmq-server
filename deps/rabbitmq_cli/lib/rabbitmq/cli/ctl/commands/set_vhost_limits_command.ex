## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.

defmodule RabbitMQ.CLI.Ctl.Commands.SetVhostLimitsCommand do
  alias RabbitMQ.CLI.Core.{DocGuide, Helpers}

  @behaviour RabbitMQ.CLI.CommandBehaviour

  def merge_defaults(args, opts) do
    {args, Map.merge(%{vhost: "/"}, opts)}
  end

  use RabbitMQ.CLI.Core.AcceptsOnePositionalArgument
  use RabbitMQ.CLI.Core.RequiresRabbitAppRunning

  def run([definition], %{node: node_name, vhost: vhost}) do
    :rabbit_misc.rpc_call(node_name, :rabbit_vhost_limit, :parse_set, [
      vhost,
      definition,
      Helpers.cli_acting_user()
    ])
  end

  use RabbitMQ.CLI.DefaultOutput

  def usage, do: "set_vhost_limits [--vhost <vhost>] <definition>"

  def usage_additional() do
    [
      ["<definition>", "Limit definitions, must be a valid JSON document"]
    ]
  end

  def usage_doc_guides() do
    [
      DocGuide.virtual_hosts()
    ]
  end

  def help_section(), do: :virtual_hosts

  def description(), do: "Sets virtual host limits"

  def banner([definition], %{vhost: vhost}) do
    "Setting vhost limits to \"#{definition}\" for vhost \"#{vhost}\" ..."
  end
end
