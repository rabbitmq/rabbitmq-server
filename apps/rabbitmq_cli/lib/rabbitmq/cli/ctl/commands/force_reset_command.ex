## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.

defmodule RabbitMQ.CLI.Ctl.Commands.ForceResetCommand do
  alias RabbitMQ.CLI.Core.{DocGuide, ExitCodes}

  @behaviour RabbitMQ.CLI.CommandBehaviour

  use RabbitMQ.CLI.Core.MergesNoDefaults
  use RabbitMQ.CLI.Core.AcceptsNoPositionalArguments
  use RabbitMQ.CLI.Core.RequiresRabbitAppStopped

  def run([], %{node: node_name}) do
    :rabbit_misc.rpc_call(node_name, :rabbit_mnesia, :force_reset, [])
  end

  def output({:error, :mnesia_unexpectedly_running}, %{node: node_name}) do
    {:error, ExitCodes.exit_software(),
              RabbitMQ.CLI.DefaultOutput.mnesia_running_error(node_name)}
  end

  use RabbitMQ.CLI.DefaultOutput

  def usage, do: "force_reset"

  def usage_doc_guides() do
    [
      DocGuide.clustering()
    ]
  end

  def help_section(), do: :cluster_management

  def description(), do: "Forcefully returns a RabbitMQ node to its virgin state"

  def banner(_, %{node: node_name}), do: "Forcefully resetting node #{node_name} ..."
end
