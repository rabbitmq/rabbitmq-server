## The contents of this file are subject to the Mozilla Public License
## Version 1.1 (the "License"); you may not use this file except in
## compliance with the License. You may obtain a copy of the License
## at http://www.mozilla.org/MPL/
##
## Software distributed under the License is distributed on an "AS IS"
## basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
## the License for the specific language governing rights and
## limitations under the License.
##
## The Original Code is RabbitMQ.
##
## The Initial Developer of the Original Code is Pivotal Software, Inc.
## Copyright (c) 2016-2017 Pivotal Software, Inc.  All rights reserved.

alias RabbitMQ.CLI.Core.Config

defmodule RabbitMQ.CLI.Core.Distribution do

  #
  # API
  #
  def start() do
    start(%{})
  end

  def start(options) do
    node_name_type = Config.get_option(:longnames, options)
    result = start(node_name_type, 10, :undefined)
    ensure_cookie(options)
    result
  end

  def start_as(node_name, options) do
    node_name_type = Config.get_option(:longnames, options)
    result = start_with_epmd(node_name, node_name_type)
    ensure_cookie(options)
    result
  end

  ## Optimization. We try to start EPMD only if distribution fails
  def start_with_epmd(node_name, node_name_type) do
    case :net_kernel.start([node_name, node_name_type]) do
      {:ok, _} = ok -> ok;
      {:error, {:already_started, _}} = started -> started;
      {:error, {{:already_started, _}, _}} = started -> started;
      ## EPMD can be stopped. Retry with EPMD
      {:error, _} ->
        :rabbit_nodes_common.ensure_epmd()
        :net_kernel.start([node_name, node_name_type])
    end
  end

  #
  # Implementation
  #

  def ensure_cookie(options) do
    case Config.get_option(:erlang_cookie, options) do
      nil    -> :ok;
      cookie -> Node.set_cookie(cookie)
                :ok
    end
  end

  defp start(_opt, 0, last_err) do
    {:error, last_err}
  end

  defp start(node_name_type, attempts, _last_err) do
    candidate = generate_cli_node_name(node_name_type)
    case start_with_epmd(candidate, node_name_type) do
      {:ok, _} -> :ok
      {:error, {:already_started, pid}}      -> {:ok, pid};
      {:error, {{:already_started, pid}, _}} -> {:ok, pid};
      {:error, reason} ->
        start(node_name_type, attempts - 1, reason)
    end
  end

  defp generate_cli_node_name(node_name_type) do
    base                 = "rabbitmqcli" <> to_string(:rabbit_misc.random(100))
    inet_resolver_config = :inet.get_rc()

    case {node_name_type, Keyword.get(inet_resolver_config, :domain)} do
      {:longnames, nil} ->
        generate_dot_no_domain_name(base);
      {:longnames, ""}  ->
        generate_dot_no_domain_name(base);
      _ ->
        base
    end |> String.to_atom

  end

  defp generate_dot_no_domain_name(base) do
    # Distribution will fail to start if it's unable to
    # determine FQDN of a node (with at least one dot in
    # the name).
    # The CLI always acts as a connection initiator, so it
    # doesn't matter if the name will not resolve.
    base <> "@" <> to_string(:inet_db.gethostname()) <> ".no-domain"
  end
end
