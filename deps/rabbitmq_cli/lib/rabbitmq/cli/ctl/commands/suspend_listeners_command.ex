## The contents of this file are subject to the Mozilla Public License
## Version 1.1 (the "License"); you may not use this file except in
## compliance with the License. You may obtain a copy of the License
## at https://www.mozilla.org/MPL/
##
## Software distributed under the License is distributed on an "AS IS"
## basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
## the License for the specific language governing rights and
## limitations under the License.
##
## The Original Code is RabbitMQ.
##
## The Initial Developer of the Original Code is GoPivotal, Inc.
## Copyright (c) 2007-2020 VMware, Inc. or its affiliates.  All rights reserved.

defmodule RabbitMQ.CLI.Ctl.Commands.SuspendListenersCommand do
  @moduledoc """
  Suspends all client connection listeners. Suspended listeners will not
  accept any new connections but already established ones will not be interrupted.
  `ResumeListenersCommand` will undo the effect of this command.

  This command is meant to be used when automating upgrades.
  See also `ResumeListenersCommand`.
  """

  @behaviour RabbitMQ.CLI.CommandBehaviour

  alias RabbitMQ.CLI.Core.DocGuide

  use RabbitMQ.CLI.Core.MergesNoDefaults
  use RabbitMQ.CLI.Core.AcceptsNoPositionalArguments

  def run([], %{node: node_name, timeout: timeout}) do
    :rabbit_misc.rpc_call(node_name, :rabbit_maintenance, :suspend_all_client_listeners, [], timeout)
  end

  use RabbitMQ.CLI.DefaultOutput

  def usage, do: "suspend_listeners"

  def usage_doc_guides() do
    [
      DocGuide.upgrade()
    ]
  end

  def help_section(), do: :operations

  def description(), do: "Suspends client connection listeners so that no new client connections are accepted"

  def banner(_, %{node: node_name}) do
    "Will suspend client connection listeners on node #{node_name}. "
    <> "The node will no longer accept client connections!"
  end
end
