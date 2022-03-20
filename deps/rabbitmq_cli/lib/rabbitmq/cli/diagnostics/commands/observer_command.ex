## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.

defmodule RabbitMQ.CLI.Diagnostics.Commands.ObserverCommand do
  @behaviour RabbitMQ.CLI.CommandBehaviour
  use RabbitMQ.CLI.DefaultOutput

  def switches(), do: [interval: :integer]
  def aliases(), do: [i: :interval]

  def merge_defaults(args, opts) do
    {args, Map.merge(%{interval: 5}, opts)}
  end


  use RabbitMQ.CLI.Core.AcceptsNoPositionalArguments

  def run([], %{node: node_name, interval: interval}) do
    case :observer_cli.start(node_name, [{:interval, interval * 1000}]) do
      # See zhongwencool/observer_cli#54
      {:badrpc, _} = err   -> err
      {:error, _} = err    -> err
      {:error, _, _} = err -> err
      :ok   -> {:ok, "Disconnected from #{node_name}."}
      :quit -> {:ok, "Disconnected from #{node_name}."}
      other -> other
    end
  end

  def help_section(), do: :observability_and_health_checks

  def description(), do: "Starts a CLI observer interface on the target node"

  def usage, do: "observer [--interval <seconds>]"

  def usage_additional() do
    [
      ["--interval <seconds>", "Update interval to use, in seconds"]
    ]
  end

  def banner(_, %{node: node_name}) do
    "Starting a CLI observer interface on node #{node_name}..."
  end
end
