## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2019-2022 VMware, Inc. or its affiliates.  All rights reserved.

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
