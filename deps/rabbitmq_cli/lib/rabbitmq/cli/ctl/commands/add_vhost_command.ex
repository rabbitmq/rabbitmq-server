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
