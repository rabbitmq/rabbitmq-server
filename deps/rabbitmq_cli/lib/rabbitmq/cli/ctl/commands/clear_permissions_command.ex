## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.

defmodule RabbitMQ.CLI.Ctl.Commands.ClearPermissionsCommand do
  alias RabbitMQ.CLI.Core.{DocGuide, ExitCodes, Helpers}

  @behaviour RabbitMQ.CLI.CommandBehaviour

  def merge_defaults(args, opts) do
    {args, Map.merge(%{vhost: "/"}, opts)}
  end

  use RabbitMQ.CLI.Core.AcceptsOnePositionalArgument
  use RabbitMQ.CLI.Core.RequiresRabbitAppRunning

  def run([username], %{node: node_name, vhost: vhost}) do
    :rabbit_misc.rpc_call(node_name, :rabbit_auth_backend_internal, :clear_permissions, [
      username,
      vhost,
      Helpers.cli_acting_user()
    ])
  end

  def output({:error, {:no_such_user, username}}, %{node: node_name, formatter: "json"}) do
    {:error, %{"result" => "error", "node" => node_name, "message" => "User #{username} does not exist"}}
  end
  def output({:error, {:no_such_vhost, vhost}}, %{node: node_name, formatter: "json"}) do
    {:error, %{"result" => "error", "node" => node_name, "message" => "Virtual host #{vhost} does not exist"}}
  end
  def output({:error, {:no_such_user, username}}, _) do
    {:error, ExitCodes.exit_nouser(), "User #{username} does not exist"}
  end
  def output({:error, {:no_such_vhost, vhost}}, _) do
    {:error, "Virtual host #{vhost} does not exist"}
  end
  use RabbitMQ.CLI.DefaultOutput

  def usage, do: "clear_permissions [--vhost <vhost>] <username>"

  def usage_additional() do
    [
      ["<username>", "Name of the user whose permissions should be revoked"]
    ]
  end

  def usage_doc_guides() do
    [
      DocGuide.access_control()
    ]
  end

  def help_section(), do: :access_control
  def description(), do: "Revokes user permissions for a vhost"

  def banner([username], %{vhost: vhost}) do
    "Clearing permissions for user \"#{username}\" in vhost \"#{vhost}\" ..."
  end
end
