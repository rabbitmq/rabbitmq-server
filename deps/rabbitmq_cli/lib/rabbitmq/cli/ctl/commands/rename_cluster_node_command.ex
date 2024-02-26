## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2024 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.

defmodule RabbitMQ.CLI.Ctl.Commands.RenameClusterNodeCommand do
  require Integer
  alias RabbitMQ.CLI.Core.DocGuide

  @behaviour RabbitMQ.CLI.CommandBehaviour

  def merge_defaults(args, opts), do: {args, opts}

  def validate(args, _) when length(args) < 2 do
    {:validation_failure, :not_enough_args}
  end

  def validate(_, _) do
    :ok
  end

  def run(_nodes, %{node: _node_name}) do
    :ok
  end

  use RabbitMQ.CLI.DefaultOutput

  def usage() do
    "rename_cluster_node <oldnode1> <newnode1> [oldnode2] [newnode2] ..."
  end

  def usage_doc_guides() do
    [
      DocGuide.clustering()
    ]
  end

  def help_section(), do: :deprecated

  def description() do
    "DEPRECATED. This command is a no-op. Node renaming is incompatible with Raft-based features such as quorum queues, streams, Khepri. "
  end

  def banner(_, _opts) do
    "DEPRECATED. This command is a no-op. Node renaming is incompatible with Raft-based features such as quorum queues, streams, Khepri. "
  end
end
