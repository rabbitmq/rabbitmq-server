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
## Copyright (c) 2007-2019 Pivotal Software, Inc.  All rights reserved.

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
