## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.

defmodule RabbitMQ.CLI.Diagnostics.Commands.ServerVersionCommand do
  @behaviour RabbitMQ.CLI.CommandBehaviour

  use RabbitMQ.CLI.Core.AcceptsDefaultSwitchesAndTimeout
  use RabbitMQ.CLI.Core.MergesNoDefaults
  use RabbitMQ.CLI.Core.AcceptsNoPositionalArguments

  def run([], %{node: node_name, timeout: timeout}) do
    :rabbit_data_coercion.to_binary(
      :rabbit_misc.rpc_call(node_name, :rabbit_misc, :version, [], timeout))
  end

  def output(result, %{formatter: "json"}) do
    {:ok, %{"result" => "ok", "value" => result}}
  end
  def output(result, _options) when is_bitstring(result) do
    {:ok, result}
  end

  use RabbitMQ.CLI.DefaultOutput

  def help_section(), do: :observability_and_health_checks

  def description(), do: "Displays server version on the target node"

  def usage, do: "server_version"

  def banner([], %{node: node_name}) do
    "Asking node #{node_name} for its RabbitMQ version..."
  end
end
