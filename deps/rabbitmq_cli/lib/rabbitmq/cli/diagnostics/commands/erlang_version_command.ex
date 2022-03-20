## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.

defmodule RabbitMQ.CLI.Diagnostics.Commands.ErlangVersionCommand do
  @behaviour RabbitMQ.CLI.CommandBehaviour

  def switches() do
    [details: :boolean, offline: :boolean, timeout: :integer]
  end
  def aliases(), do: [t: :timeout]

  def merge_defaults(args, opts) do
    {args, Map.merge(%{details: false, offline: false}, opts)}
  end

  use RabbitMQ.CLI.Core.AcceptsNoPositionalArguments

  def run([], %{details: details, offline: true}) do
    case details do
      true ->
        :rabbit_data_coercion.to_binary(
          :rabbit_misc.otp_system_version())

      false ->
        :rabbit_data_coercion.to_binary(
          :rabbit_misc.platform_and_version())
    end
  end
  def run([], %{node: node_name, timeout: timeout, details: details}) do
    case details do
      true ->
        :rabbit_data_coercion.to_binary(
          :rabbit_misc.rpc_call(node_name, :rabbit_misc, :otp_system_version, [], timeout))

      false ->
      :rabbit_data_coercion.to_binary(
        :rabbit_misc.rpc_call(node_name, :rabbit_misc, :platform_and_version, [], timeout))
    end
  end

  def output(result, %{formatter: "json"}) do
    {:ok, %{"result" => "ok", "value" => result}}
  end
  def output(result, _opts) when is_bitstring(result) do
    {:ok, result}
  end

  use RabbitMQ.CLI.DefaultOutput

  def help_section(), do: :observability_and_health_checks

  def description(), do: "Displays Erlang/OTP version on the target node"

  def usage, do: "erlang_version"

  def usage_additional() do
    [
      ["--details", "when set, display additional Erlang/OTP system information"],
      ["--offline", "when set, displays local Erlang/OTP version (that used by CLI tools)"]
    ]
  end

  def banner([], %{offline: true}) do
    "CLI Erlang/OTP version ..."
  end
  def banner([], %{node: node_name}) do
    "Asking node #{node_name} for its Erlang/OTP version..."
  end
end
