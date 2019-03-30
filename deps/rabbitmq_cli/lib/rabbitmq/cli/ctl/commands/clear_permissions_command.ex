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

defmodule RabbitMQ.CLI.Ctl.Commands.ClearPermissionsCommand do
  alias RabbitMQ.CLI.Core.{DocGuide, Helpers}

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
