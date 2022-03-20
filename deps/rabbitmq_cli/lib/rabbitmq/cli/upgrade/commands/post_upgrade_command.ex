## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.

defmodule RabbitMQ.CLI.Upgrade.Commands.PostUpgradeCommand do
  alias RabbitMQ.CLI.Core.DocGuide

  @behaviour RabbitMQ.CLI.CommandBehaviour

  use RabbitMQ.CLI.Core.RequiresRabbitAppRunning
  use RabbitMQ.CLI.Core.MergesNoDefaults
  use RabbitMQ.CLI.Core.AcceptsNoPositionalArguments

  def run([], %{node: node_name}) do
    :rabbit_misc.rpc_call(node_name, :rabbit_amqqueue, :rebalance, [:all, ".*", ".*"])
  end

  use RabbitMQ.CLI.DefaultOutput

  def usage, do: "post_upgrade"

  def usage_doc_guides() do
    [
      DocGuide.upgrade()
    ]
  end

  def help_section, do: :upgrade

  def description, do: "Runs post-upgrade tasks"

  def banner([], _) do
    "Executing post upgrade tasks...\n" <>
    "Rebalancing queue masters..."
  end

end
