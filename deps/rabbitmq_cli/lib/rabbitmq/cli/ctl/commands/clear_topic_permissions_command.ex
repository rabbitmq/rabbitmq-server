## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.

defmodule RabbitMQ.CLI.Ctl.Commands.ClearTopicPermissionsCommand do
  alias RabbitMQ.CLI.Core.{DocGuide, ExitCodes, Helpers}

  @behaviour RabbitMQ.CLI.CommandBehaviour

  def merge_defaults(args, opts) do
    {args, Map.merge(%{vhost: "/"}, opts)}
  end

  def validate([], _) do
    {:validation_failure, :not_enough_args}
  end

  def validate([_ | _] = args, _) when length(args) > 2 do
    {:validation_failure, :too_many_args}
  end

  def validate([_], _), do: :ok
  def validate([_, _], _), do: :ok

  use RabbitMQ.CLI.Core.RequiresRabbitAppRunning

  def run([username], %{node: node_name, vhost: vhost}) do
    :rabbit_misc.rpc_call(node_name, :rabbit_auth_backend_internal, :clear_topic_permissions, [
      username,
      vhost,
      Helpers.cli_acting_user()
    ])
  end

  def run([username, exchange], %{node: node_name, vhost: vhost}) do
    :rabbit_misc.rpc_call(node_name, :rabbit_auth_backend_internal, :clear_topic_permissions, [
      username,
      vhost,
      exchange,
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

  def usage, do: "clear_topic_permissions [--vhost <vhost>] <username> [<exchange>]"

  def usage_additional() do
    [
      ["<username>", "Name of the user whose topic permissions should be revoked"],
      ["<exchange>", "Exchange the permissions are for"]
    ]
  end

  def usage_doc_guides() do
    [
      DocGuide.access_control()
    ]
  end

  def help_section(), do: :access_control
  def description(), do: "Clears user topic permissions for a vhost or exchange"

  def banner([username], %{vhost: vhost}) do
    "Clearing topic permissions for user \"#{username}\" in vhost \"#{vhost}\" ..."
  end

  def banner([username, exchange], %{vhost: vhost}) do
    "Clearing topic permissions on \"#{exchange}\" for user \"#{username}\" in vhost \"#{vhost}\" ..."
  end
end
