## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.


defmodule RabbitMQ.CLI.Streams.Commands.SetStreamRetentionPolicyCommand do
  alias RabbitMQ.CLI.Core.DocGuide

  @behaviour RabbitMQ.CLI.CommandBehaviour

  def merge_defaults(args, opts), do: {args, Map.merge(%{vhost: "/"}, opts)}

  use RabbitMQ.CLI.Core.AcceptsTwoPositionalArguments
  use RabbitMQ.CLI.Core.RequiresRabbitAppRunning

  def run([name, retention_policy], %{node: node_name, vhost: vhost}) do
    :rabbit_misc.rpc_call(node_name, :rabbit_stream_queue, :set_retention_policy, [
          name,
          vhost,
          retention_policy
    ])
  end

  use RabbitMQ.CLI.DefaultOutput

  def banner([name, retention_policy], _) do
    "Setting retention policy of stream queue #{name} to #{retention_policy} ..."
  end

  def usage, do: "set_stream_retention_policy [--vhost <vhost>] <name> <policy>"

  def usage_additional() do
    [
      ["<name>", "stream queue name"],
      ["<policy>", "retention policy"]
    ]
  end

  def usage_doc_guides() do
    [
      DocGuide.streams()
    ]
  end

  def help_section(), do: :policies

  def description(), do: "Sets the retention policy of a stream queue"
end
