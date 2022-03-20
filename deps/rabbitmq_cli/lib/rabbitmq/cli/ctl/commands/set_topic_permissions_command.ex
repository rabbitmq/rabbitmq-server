## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.

defmodule RabbitMQ.CLI.Ctl.Commands.SetTopicPermissionsCommand do
  alias RabbitMQ.CLI.Core.{DocGuide, ExitCodes, Helpers}

  @behaviour RabbitMQ.CLI.CommandBehaviour

  def merge_defaults(args, opts) do
    {args, Map.merge(%{vhost: "/"}, opts)}
  end

  def validate(args, _) when length(args) < 4 do
    {:validation_failure, :not_enough_args}
  end
  def validate(args, _) when length(args) > 4 do
    {:validation_failure, :too_many_args}
  end
  def validate(_, _), do: :ok

  use RabbitMQ.CLI.Core.RequiresRabbitAppRunning

  def run([user, exchange, write_pattern, read_pattern], %{node: node_name, vhost: vhost}) do
    :rabbit_misc.rpc_call(
      node_name,
      :rabbit_auth_backend_internal,
      :set_topic_permissions,
      [user, vhost, exchange, write_pattern, read_pattern, Helpers.cli_acting_user()]
    )
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

  def usage do
    "set_topic_permissions [--vhost <vhost>] <username> <exchange> <write> <read>"
  end

  def usage_additional do
    [
      ["<username>", "Self-explanatory"],
      ["<exchange>", "Topic exchange to set the permissions for"],
      ["<write>", "Write permission pattern"],
      ["<read>", "Read permission pattern"]
    ]
  end

  def usage_doc_guides() do
    [
      DocGuide.access_control()
    ]
  end

  def help_section(), do: :access_control

  def description(), do: "Sets user topic permissions for an exchange"

  def banner([user, exchange, _, _], %{vhost: vhost}),
    do:
      "Setting topic permissions on \"#{exchange}\" for user \"#{user}\" in vhost \"#{vhost}\" ..."
end
