## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.

defmodule RabbitMQ.CLI.Ctl.Commands.StopAppCommand do
  @behaviour RabbitMQ.CLI.CommandBehaviour

  use RabbitMQ.CLI.Core.MergesNoDefaults
  use RabbitMQ.CLI.Core.AcceptsNoPositionalArguments

  def run([], %{node: node_name}) do
    :rabbit_misc.rpc_call(node_name, :rabbit, :stop, [])
  end

  use RabbitMQ.CLI.DefaultOutput

  def usage, do: "stop_app"

  def help_section(), do: :node_management

  def description(), do: "Stops the RabbitMQ application, leaving the runtime (Erlang VM) running"

  def banner(_, %{node: node_name}), do: "Stopping rabbit application on node #{node_name} ..."
end
