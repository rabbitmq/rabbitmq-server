## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries.  All rights reserved.

defmodule RabbitMQ.CLI.Streams.Commands.SetStreamRetentionPolicyCommand do
  alias RabbitMQ.CLI.Core.DocGuide

  @behaviour RabbitMQ.CLI.CommandBehaviour

  def merge_defaults(args, opts), do: {args, Map.merge(%{vhost: "/"}, opts)}

  use RabbitMQ.CLI.Core.AcceptsTwoPositionalArguments
  use RabbitMQ.CLI.Core.RequiresRabbitAppRunning

  def run(_, _) do
    :ok
  end

  use RabbitMQ.CLI.DefaultOutput

  def banner(_, _) do
    "DEPRECATED. This command is a no-op. Use a policy to set data retention."
  end

  def usage, do: "set_stream_retention_policy [--vhost <vhost>] <name> <policy>"

  def usage_doc_guides() do
    [
      DocGuide.streams()
    ]
  end

  def help_section(), do: :policies

  def description(), do: "DEPRECATED. This command is a no-op. Use a policy to set data retention."
end
