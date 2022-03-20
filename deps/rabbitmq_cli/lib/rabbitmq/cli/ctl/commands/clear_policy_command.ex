## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.

defmodule RabbitMQ.CLI.Ctl.Commands.ClearPolicyCommand do
  alias RabbitMQ.CLI.Core.{Helpers, DocGuide}

  @behaviour RabbitMQ.CLI.CommandBehaviour
  use RabbitMQ.CLI.DefaultOutput

  def merge_defaults(args, opts) do
    {args, Map.merge(%{vhost: "/"}, opts)}
  end

  use RabbitMQ.CLI.Core.AcceptsOnePositionalArgument
  use RabbitMQ.CLI.Core.RequiresRabbitAppRunning

  def run([key], %{node: node_name, vhost: vhost}) do
    :rabbit_misc.rpc_call(node_name, :rabbit_policy, :delete, [
      vhost,
      key,
      Helpers.cli_acting_user()
    ])
  end

  def usage, do: "clear_policy [--vhost <vhost>] <name>"

  def usage_additional() do
    [
      ["<name>", "Name of policy to clear (remove)"]
    ]
  end

  def usage_doc_guides() do
    [
      DocGuide.parameters()
    ]
  end

  def help_section(), do: :policies

  def description(), do: "Clears (removes) a policy"

  def banner([key], %{vhost: vhost}) do
    "Clearing policy \"#{key}\" on vhost \"#{vhost}\" ..."
  end
end
