## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries.  All rights reserved.

defmodule RabbitMQ.CLI.Ctl.Commands.DisableVhostDeletionProtectionCommand do
  alias RabbitMQ.CLI.Core.{DocGuide, Helpers}

  @behaviour RabbitMQ.CLI.CommandBehaviour

  @metadata_key :protected_from_deletion

  def switches(), do: []
  def aliases(), do: []

  def merge_defaults(args, opts) do
    {args, opts}
  end

  use RabbitMQ.CLI.Core.RequiresRabbitAppRunning
  use RabbitMQ.CLI.Core.AcceptsOnePositionalArgument

  def run([vhost], %{node: node_name}) do
    metadata_patch = %{
      @metadata_key => false
    }
    :rabbit_misc.rpc_call(node_name, :rabbit_vhost, :update_metadata, [
      vhost,
      metadata_patch,
      Helpers.cli_acting_user()
    ])
  end

  use RabbitMQ.CLI.DefaultOutput

  def usage,
    do:
      "disable_vhost_deletion_protection <vhost>"

  def usage_additional() do
    [
      ["<vhost>", "Virtual host name"]
    ]
  end

  def usage_doc_guides() do
    [
      DocGuide.virtual_hosts()
    ]
  end

  def help_section(), do: :virtual_hosts

  def description(), do: "Removes deletion protection from a virtual host (so that it can be deleted)"

  def banner([vhost], _), do: "Removing deletion protection from virtual host \"#{vhost}\" by updating its metadata..."
end
