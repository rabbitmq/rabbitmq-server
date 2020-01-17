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
