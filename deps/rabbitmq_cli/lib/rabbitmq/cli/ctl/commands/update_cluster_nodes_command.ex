## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2024 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.

defmodule RabbitMQ.CLI.Ctl.Commands.UpdateClusterNodesCommand do
  alias RabbitMQ.CLI.Core.DocGuide

  @behaviour RabbitMQ.CLI.CommandBehaviour

  use RabbitMQ.CLI.Core.MergesNoDefaults
  use RabbitMQ.CLI.Core.AcceptsOnePositionalArgument
  use RabbitMQ.CLI.Core.RequiresRabbitAppStopped

  def run([_seed_node], _opts) do
    :ok
  end

  use RabbitMQ.CLI.DefaultOutput

  def usage() do
    "update_cluster_nodes <seed_node>"
  end

  def usage_additional() do
    [
      ["<seed_node>", "Cluster node to seed known cluster members from"]
    ]
  end

  def usage_doc_guides() do
    [
      DocGuide.clustering()
    ]
  end

  def help_section(), do: :deprecated

  def description() do
    "DEPRECATED. This command is a no-op. Node update is incompatible with Raft-based features such as quorum queues, streams, Khepri. "
  end

  def banner(_, _opts) do
    "DEPRECATED. This command is a no-op. Node update is incompatible with Raft-based features such as quorum queues, streams, Khepri. "
  end
end
