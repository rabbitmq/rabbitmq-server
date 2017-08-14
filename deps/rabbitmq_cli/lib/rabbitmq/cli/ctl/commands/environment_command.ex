## The contents of this file are subject to the Mozilla Public License
## Version 1.1 (the "License"); you may not use this file except in
## compliance with the License. You may obtain a copy of the License
## at http://www.mozilla.org/MPL/
##
## Software distributed under the License is distributed on an "AS IS"
## basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
## the License for the specific language governing rights and
## limitations under the License.
##
## The Original Code is RabbitMQ.
##
## The Initial Developer of the Original Code is GoPivotal, Inc.
## Copyright (c) 2007-2017 Pivotal Software, Inc.  All rights reserved.


defmodule RabbitMQ.CLI.Ctl.Commands.EnvironmentCommand do
  @behaviour RabbitMQ.CLI.CommandBehaviour
  use RabbitMQ.CLI.DefaultOutput

  def formatter(), do: RabbitMQ.CLI.Formatters.Erlang

  def scopes(), do: [:ctl, :diagnostics]

  def merge_defaults(args, opts), do: {args, opts}

  def validate([_|_], _), do: {:validation_failure, :too_many_args}
  def validate(_, _), do: :ok

  def run([], %{node: node_name}) do
    :rabbit_misc.rpc_call(node_name, :rabbit, :environment, [])
  end

  def usage, do: "environment"

  def banner(_, %{node: node_name}), do: "Application environment of node #{node_name} ..."
end
