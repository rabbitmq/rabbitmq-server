## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.

defmodule RabbitMQ.CLI.Ctl.Commands.ListTopicPermissionsCommand do
  alias RabbitMQ.CLI.Core.DocGuide

  @behaviour RabbitMQ.CLI.CommandBehaviour

  def scopes(), do: [:ctl, :diagnostics]
  use RabbitMQ.CLI.Core.AcceptsDefaultSwitchesAndTimeout

  def merge_defaults(args, opts) do
    {args, Map.merge(%{vhost: "/", table_headers: true}, opts)}
  end

  use RabbitMQ.CLI.Core.AcceptsNoPositionalArguments
  use RabbitMQ.CLI.Core.RequiresRabbitAppRunning

  def run([], %{node: node_name, timeout: timeout, vhost: vhost}) do
    :rabbit_misc.rpc_call(
      node_name,
      :rabbit_auth_backend_internal,
      :list_vhost_topic_permissions,
      [vhost],
      timeout
    )
  end

  use RabbitMQ.CLI.DefaultOutput

  def formatter(), do: RabbitMQ.CLI.Formatters.Table

  def usage, do: "list_topic_permissions [--vhost <vhost>] [--no-table-headers]"

  def usage_doc_guides() do
    [
      DocGuide.access_control()
    ]
  end

  def help_section(), do: :access_control
  def description(), do: "Lists topic permissions in a virtual host"

  def banner(_, %{vhost: vhost}), do: "Listing topic permissions for vhost \"#{vhost}\" ..."
end
