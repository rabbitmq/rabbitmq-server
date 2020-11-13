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
## Copyright (c) 2019 Pivotal Software, Inc.  All rights reserved.

defmodule RabbitMQ.CLI.Diagnostics.Commands.LogLocationCommand do
  @moduledoc """
  Displays standard log file location on the target node
  """
  @behaviour RabbitMQ.CLI.CommandBehaviour

  alias RabbitMQ.CLI.Core.LogFiles

  def switches, do: [all: :boolean, timeout: :integer]
  def aliases, do: [a: :all, t: :timeout]

  def merge_defaults(args, opts) do
    {args, Map.merge(%{all: false}, opts)}
  end

  use RabbitMQ.CLI.Core.AcceptsNoPositionalArguments

  def run([], %{node: node_name, timeout: timeout, all: all}) do
    case all do
      true  -> LogFiles.get_log_locations(node_name, timeout);
      false -> LogFiles.get_default_log_location(node_name, timeout)
    end
  end

  def output({:ok, location}, %{node: node_name, formatter: "json"}) do
    {:ok, %{
      "result" => "ok",
      "node_name" => node_name,
      "paths"  => [location]
    }}
  end
  def output(locations, %{node: node_name, formatter: "json"}) do
    {:ok, %{
      "result" => "ok",
      "node_name" => node_name,
      "paths"  => locations
    }}
  end
  use RabbitMQ.CLI.DefaultOutput

  def help_section(), do: :configuration

  def description(), do: "Shows log file location(s) on target node"

  def usage, do: "log_location [--all|-a]"

  def banner([], %{node: node_name}) do
    "Log file location(s) on node #{node_name} ..."
  end
end
