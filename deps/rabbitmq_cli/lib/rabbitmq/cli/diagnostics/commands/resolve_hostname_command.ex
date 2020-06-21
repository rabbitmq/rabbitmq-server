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
## Copyright (c) 2007-2020 VMware, Inc. or its affiliates.  All rights reserved.

defmodule RabbitMQ.CLI.Diagnostics.Commands.ResolveHostnameCommand do
  @moduledoc """
  Resolves a hostname to one or more addresses of a given IP address family (IPv4 ot IPv6).
  This command is not meant to compete with `dig` but rather provide a way
  to perform basic resolution tests that take Erlang's inetrc file into account.
  """

  import RabbitCommon.Records
  alias RabbitMQ.CLI.Core.Networking
  alias RabbitMQ.CLI.Core.ExitCodes

  @behaviour RabbitMQ.CLI.CommandBehaviour

  def scopes(), do: [:diagnostics]

  def switches(), do: [address_family: :string, offline: :boolean]
  def aliases(), do: [a: :address_family]

  def merge_defaults(args, opts) do
    {args, Map.merge(%{address_family: "IPv4", offline: false}, opts)}
  end

  def validate(args, _) when length(args) < 1, do: {:validation_failure, :not_enough_args}
  def validate(args, _) when length(args) > 1, do: {:validation_failure, :too_many_args}
  def validate([_], %{address_family: family}) do
    case Networking.valid_address_family?(family) do
      true  -> :ok
      false -> {:validation_failure, {:bad_argument, "unsupported IP address family #{family}. Valid values are: ipv4, ipv6"}}
    end
  end
  def validate([_], _), do: :ok

  def run([hostname], %{address_family: family, offline: true}) do
    :inet.gethostbyname(to_charlist(hostname), Networking.address_family(family))
  end
  def run([hostname], %{node: node_name, address_family: family, offline: false, timeout: timeout}) do
    case :rabbit_misc.rpc_call(node_name, :inet, :gethostbyname,
           [to_charlist(hostname), Networking.address_family(family)], timeout) do
      {:error, _} = err -> err
      {:error, _, _} = err -> err
      {:ok, result} -> {:ok, result}
      other -> other
    end
  end

  def output({:error, :nxdomain}, %{node: node_name, formatter: "json"}) do
    m = %{
      "result"  => "error",
      "node"    => node_name,
      "message" => "Hostname does not resolve (resolution failed with an nxdomain)"
    }
    {:error, ExitCodes.exit_dataerr(), m}
  end
  def output({:error, :nxdomain}, _opts) do
    {:error, ExitCodes.exit_dataerr(), "Hostname does not resolve (resolution failed with an nxdomain)"}
  end
  def output({:ok, result}, %{node: node_name, address_family: family, formatter: "json"}) do
    hostname  = hostent(result, :h_name)
    addresses = hostent(result, :h_addr_list)
    {:ok, %{
      "result"         => "ok",
      "node"           => node_name,
      "hostname"       => to_string(hostname),
      "address_family" => family,
      "addresses"      => Networking.format_addresses(addresses)
    }}
  end
  def output({:ok, result}, _opts) do
    addresses = hostent(result, :h_addr_list)
    {:ok, Enum.join(Networking.format_addresses(addresses), "\n")}
  end
  use RabbitMQ.CLI.DefaultOutput

  def usage() do
    "resolve_hostname <hostname> [--address-family <ipv4 | ipv6>]"
  end

  def help_section(), do: :observability_and_health_checks

  def description(), do: "Resolves a hostname to a set of addresses. Takes Erlang's inetrc file into account."

  def banner([hostname], %{offline: false, node: node_name, address_family: family}) do
    "Asking node #{node_name} to resolve hostname #{hostname} to #{family} addresses..."
  end
  def banner([hostname], %{offline: true, address_family: family}) do
    "Resolving hostname #{hostname} to #{family} addresses..."
  end
end
