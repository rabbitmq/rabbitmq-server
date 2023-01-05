## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2023 VMware, Inc. or its affiliates.  All rights reserved.

defmodule RabbitMQ.CLI.Ctl.Commands.ForceStandaloneKhepriBootCommand do
  alias RabbitMQ.CLI.Core.{Config, DocGuide}

  @behaviour RabbitMQ.CLI.CommandBehaviour

  use RabbitMQ.CLI.Core.MergesNoDefaults
  use RabbitMQ.CLI.Core.AcceptsNoPositionalArguments

  def run([], %{node: node_name} = opts) do
    ret =
      :rabbit_misc.rpc_call(node_name, :rabbit_khepri, :force_shrink_member_to_current_member, [])

    case ret do
      {:badrpc, {:EXIT, {:undef, _}}} ->
        {:error, RabbitMQ.CLI.Core.ExitCodes.exit_usage(),
         "This command is not supported by node #{node_name}"}

      _ ->
        ret
    end
  end

  use RabbitMQ.CLI.DefaultOutput

  def usage, do: "force_standalone_khepri_boot"

  def usage_doc_guides() do
    [
      DocGuide.clustering()
    ]
  end

  def help_section(), do: :cluster_management

  def description(),
    do: "Forces node to start as a standalone node"

  def banner(_, _), do: nil
end
