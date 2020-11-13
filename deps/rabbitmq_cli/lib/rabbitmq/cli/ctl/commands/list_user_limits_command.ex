## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2020 VMware, Inc. or its affiliates.  All rights reserved.

defmodule RabbitMQ.CLI.Ctl.Commands.ListUserLimitsCommand do
  alias RabbitMQ.CLI.Core.DocGuide

  @behaviour RabbitMQ.CLI.CommandBehaviour

  def scopes(), do: [:ctl, :diagnostics]
  def switches(), do: [global: :boolean, user: :string]

  def merge_defaults(args, %{global: true} = opts) do
    {args, Map.merge(%{table_headers: true}, opts)}
  end

  def merge_defaults(args, opts) do
    {args, Map.merge(%{user: "guest", table_headers: true}, opts)}
  end

  use RabbitMQ.CLI.Core.AcceptsNoPositionalArguments
  use RabbitMQ.CLI.Core.RequiresRabbitAppRunning

  def run([], %{node: node_name, global: true}) do
    case :rabbit_misc.rpc_call(node_name, :rabbit_auth_backend_internal, :get_user_limits, []) do
      [] ->
        []

      {:error, err} ->
        {:error, err}

      {:badrpc, node} ->
        {:badrpc, node}

      val ->
        Enum.map(val, fn {user, val} ->
          {:ok, val_encoded} = JSON.encode(Map.new(val))
          [user: user, limits: val_encoded]
        end)
    end
  end

  def run([], %{node: node_name, user: username}) do
    case :rabbit_misc.rpc_call(node_name, :rabbit_auth_backend_internal, :get_user_limits, [username]) do
      :undefined ->
        {:error, {:no_such_user, username}}

      {:error, err} ->
        {:error, err}

      {:badrpc, node} ->
        {:badrpc, node}

      val when is_list(val) or is_map(val) ->
        {:ok, val_encoded} = JSON.encode(Map.new(val))
        val_encoded
    end
  end

  use RabbitMQ.CLI.DefaultOutput

  def formatter(), do: RabbitMQ.CLI.Formatters.Table

  def usage, do: "list_user_limits [--user <username>] [--global]"

  def usage_additional() do
    [
      ["--global", "list limits for all the users"]
    ]
  end

  def usage_doc_guides() do
    [
      DocGuide.access_control()
    ]
  end

  def help_section(), do: :user_management

  def description(), do: "Displays configured user limits"

  def banner([], %{global: true}) do
    "Listing limits for all users ..."
  end

  def banner([], %{user: username}) do
    "Listing limits for user \"#{username}\" ..."
  end
end
