## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.

defmodule RabbitMQ.CLI.Diagnostics.Commands.ErlangCookieHashCommand do
  @behaviour RabbitMQ.CLI.CommandBehaviour

  use RabbitMQ.CLI.Core.AcceptsDefaultSwitchesAndTimeout
  use RabbitMQ.CLI.Core.MergesNoDefaults
  use RabbitMQ.CLI.Core.AcceptsNoPositionalArguments

  def run([], %{node: node_name, timeout: timeout}) do
    :rabbit_data_coercion.to_binary(
      :rabbit_misc.rpc_call(node_name, :rabbit_nodes_common, :cookie_hash, [], timeout))
  end

  def output(result, %{formatter: "json"}) do
    {:ok, %{"result" => "ok", "value" => result}}
  end
  def output(result, _options) when is_bitstring(result) do
    {:ok, result}
  end

  def help_section(), do: :configuration

  def description(), do: "Displays a hash of the Erlang cookie (shared secret) used by the target node"

  def usage, do: "erlang_cookie_hash"

  def banner([], %{node: node_name}) do
    "Asking node #{node_name} its Erlang cookie hash..."
  end
end
