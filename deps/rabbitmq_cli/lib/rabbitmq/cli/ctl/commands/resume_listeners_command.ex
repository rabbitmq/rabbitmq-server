## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.

defmodule RabbitMQ.CLI.Ctl.Commands.ResumeListenersCommand do
  @moduledoc """
  Resumes all client connection listeners making them accept new client
  connections. This command is the opposite of `SuspendListenersCommand`.

  This command is meant to be used when automating upgrades.
  See also `SuspendListenersCommand`.
  """

  @behaviour RabbitMQ.CLI.CommandBehaviour

  alias RabbitMQ.CLI.Core.DocGuide

  use RabbitMQ.CLI.Core.MergesNoDefaults
  use RabbitMQ.CLI.Core.AcceptsNoPositionalArguments

  def run([], %{node: node_name, timeout: timeout}) do
    :rabbit_misc.rpc_call(node_name, :rabbit_maintenance, :resume_all_client_listeners, [], timeout)
  end

  use RabbitMQ.CLI.DefaultOutput

  def usage, do: "resume_listeners"

  def usage_doc_guides() do
    [
      DocGuide.upgrade()
    ]
  end

  def help_section(), do: :operations

  def description(), do: "Resumes client connection listeners making them accept client connections again"

  def banner(_, %{node: node_name}) do
    "Will resume client connection listeners on node #{node_name}. "
    <> "The node will now accept client connections"
  end
end
