## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.

defmodule RabbitMQ.CLI.Ctl.Commands.AddVhostCommand do
  alias RabbitMQ.CLI.Core.{DocGuide, Helpers}

  @behaviour RabbitMQ.CLI.CommandBehaviour

  def switches(), do: [description: :string,
                       tags: :string]
  def aliases(), do: [d: :description]

  def merge_defaults(args, opts) do
    {args, Map.merge(%{description: "", tags: ""}, opts)}
  end
  use RabbitMQ.CLI.Core.AcceptsOnePositionalArgument
  use RabbitMQ.CLI.Core.RequiresRabbitAppRunning

  def run([vhost], %{node: node_name, description: desc, tags: tags}) do
    :rabbit_misc.rpc_call(node_name, :rabbit_vhost, :add, [vhost, desc, parse_tags(tags), Helpers.cli_acting_user()])
  end
  def run([vhost], %{node: node_name, tags: tags}) do
    :rabbit_misc.rpc_call(node_name, :rabbit_vhost, :add, [vhost, "", parse_tags(tags), Helpers.cli_acting_user()])
  end
  def run([vhost], %{node: node_name}) do
    :rabbit_misc.rpc_call(node_name, :rabbit_vhost, :add, [vhost, Helpers.cli_acting_user()])
  end

  use RabbitMQ.CLI.DefaultOutput

  def usage, do: "add_vhost <vhost> [--description <description> --tags \"<tag1>,<tag2>,<...>\"]"

  def usage_additional() do
    [
      ["<vhost>", "Virtual host name"],
      ["--description <description>", "Virtual host description"],
      ["--tags <tags>", "Command separated list of tags"]
    ]
  end

  def usage_doc_guides() do
    [
      DocGuide.virtual_hosts()
    ]
  end

  def help_section(), do: :virtual_hosts

  def description(), do: "Creates a virtual host"

  def banner([vhost], _), do: "Adding vhost \"#{vhost}\" ..."

  #
  # Implementation
  #

  def parse_tags(tags) do
    String.split(tags, ",", trim: true)
    |> Enum.map(&String.trim/1)
    |> Enum.map(&String.to_atom/1)
  end
end
