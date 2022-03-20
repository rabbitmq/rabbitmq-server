## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.

defmodule RabbitMQ.CLI.Ctl.Commands.ListUserPermissionsCommand do
  alias RabbitMQ.CLI.Core.DocGuide

  @behaviour RabbitMQ.CLI.CommandBehaviour
  use RabbitMQ.CLI.DefaultOutput

  def formatter(), do: RabbitMQ.CLI.Formatters.Table

  def scopes(), do: [:ctl, :diagnostics]
  def switches(), do: [timeout: :integer]
  def aliases(), do: [t: :timeout]

  def merge_defaults(args, opts) do
    {args, Map.merge(%{table_headers: true}, opts)}
  end

  def validate([], _), do: {:validation_failure, :not_enough_args}
  def validate([_ | _] = args, _) when length(args) > 1, do: {:validation_failure, :too_many_args}
  def validate([_], _), do: :ok

  use RabbitMQ.CLI.Core.RequiresRabbitAppRunning

  def run([username], %{node: node_name, timeout: timeout}) do
    :rabbit_misc.rpc_call(
      node_name,
      :rabbit_auth_backend_internal,
      :list_user_permissions,
      [username],
      timeout
    )
  end

  def usage, do: "list_user_permissions [--no-table-headers] <username>"

  def usage_additional do
    [
      ["<username>", "Name of the user"]
    ]
  end

  def usage_doc_guides() do
    [
      DocGuide.access_control(),
      DocGuide.virtual_hosts()
    ]
  end

  def help_section(), do: :access_control
  def description(), do: "Lists permissions of a user across all virtual hosts"

  def banner([username], _), do: "Listing permissions for user \"#{username}\" ..."
end
