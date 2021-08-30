## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.

defmodule RabbitMQ.CLI.Core.NodeName do
  alias RabbitMQ.CLI.Core.Config

  @moduledoc """
  Provides functions for correctly constructing node names given a node type and optional base name.
  """

  @doc """
  Constructs complete node name based on :longnames or :shortnames and
  a base name, in the same manner as Erlang/OTP lib/kernel/src/net_kernel.erl
  """
  def create(base, type) do
    create_name(base, type, 1)
  end

  @doc """
  Get local hostname
  """
  def hostname, do: :inet_db.gethostname() |> List.to_string()

  @doc """
  Get hostname part of current node name
  """
  def hostname_from_node do
    [_, hostname] = split_node(Node.self())
    hostname
  end

  @doc """
  Get hostname part of given node name
  """
  def hostname_from_node(name) do
    [_, hostname] = split_node(name)
    hostname
  end

  def split_node(name) when is_atom(name) do
    split_node(to_string(name))
  end

  def split_node(name) do
    case String.split(name, "@", parts: 2) do
      ["", host] ->
        default_name = to_string(Config.default(:node))
        [default_name, host]

      [_head, _host] = rslt ->
        rslt

      [head] ->
        [head, ""]
    end
  end

  @doc """
  Get local domain. If unavailable, makes a good guess. We're using
  :inet_db here because that's what Erlang/OTP uses when it creates a node
  name:
  https://github.com/erlang/otp/blob/8ca061c3006ad69c2a8d1c835d0d678438966dfc/lib/kernel/src/net_kernel.erl#L1363-L1445
  """
  def domain do
    domain(1)
  end

  #
  # Implementation
  #

  defp domain(attempt) do
    case {attempt, :inet_db.res_option(:domain), :os.type()} do
      {1, [], _} ->
        do_load_resolv()
        domain(0)

      {0, [], {:unix, :darwin}} ->
        "local"

      {0, [], _} ->
        "localdomain"

      {_, domain, _} ->
        List.to_string(domain)
    end
  end

  defp create_name(name, long_or_short_names, attempt) do
    {head, host1} = create_hostpart(name, long_or_short_names)

    case host1 do
      {:ok, host_part} ->
        case valid_name_head(head) do
          true ->
            {:ok, String.to_atom(head <> "@" <> host_part)}

          false ->
            {:error, {:node_name, :invalid_node_name_head}}
        end

      {:error, :long} when attempt == 1 ->
        do_load_resolv()
        create_name(name, long_or_short_names, 0)

      {:error, :long} when attempt == 0 ->
        case valid_name_head(head) do
          true ->
            {:ok, String.to_atom(head <> "@" <> hostname() <> "." <> domain())}

          false ->
            {:error, {:node_name, :invalid_node_name_head}}
        end

      {:error, :hostname_not_allowed} ->
        {:error, {:node_name, :hostname_not_allowed}}

      {:error, err_type} ->
        {:error, {:node_name, err_type}}
    end
  end

  defp create_hostpart(name, long_or_short_names) do
    [head, host] = split_node(name)

    host1 =
      case {host, long_or_short_names} do
        {"", :shortnames} ->
          case :inet_db.gethostname() do
            inet_db_host when inet_db_host != [] ->
              {:ok, to_string(inet_db_host)}

            _ ->
              {:error, :short}
          end

        {"", :longnames} ->
          case {:inet_db.gethostname(), :inet_db.res_option(:domain)} do
            {inet_db_host, inet_db_domain}
            when inet_db_host != [] and inet_db_domain != [] ->
              {:ok, to_string(inet_db_host) <> "." <> to_string(inet_db_domain)}

            _ ->
              {:error, :long}
          end

        {_, type} ->
          validate_hostname(host, type)
      end

    {head, host1}
  end

  defp validate_hostname(host, :longnames) do
    case String.contains?(host, ".") do
      true ->
        validate_hostname_rx(host)

      _ ->
        validate_hostname(host <> "." <> domain(), :longnames)
    end
  end

  defp validate_hostname(host, :shortnames) do
    case String.contains?(host, ".") do
      true ->
        {:error, :short}

      _ ->
        validate_hostname_rx(host)
    end
  end

  defp validate_hostname_rx(host) do
    rx = Regex.compile!("^[!-ÿ]*$", [:unicode])

    case Regex.match?(rx, host) do
      true ->
        {:ok, host}

      false ->
        {:error, :hostname_not_allowed}
    end
  end

  defp valid_name_head(head) do
    rx = Regex.compile!("^[0-9A-Za-z_\\-]+$", [:unicode])
    Regex.match?(rx, head)
  end

  defp do_load_resolv do
    # It could be we haven't read domain name from resolv file yet
    :ok = :inet_config.do_load_resolv(:os.type(), :longnames)
  end
end
