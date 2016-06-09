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
## Copyright (c) 2007-2016 Pivotal Software, Inc.  All rights reserved.


defmodule SetParameterCommand do
  alias RabbitMQ.CLI.RabbitMQCtl.Helpers, as: Helpers

  @behaviour CommandBehaviour
  @flags [:vhost]

  def switches(), do: []
  def merge_defaults(args, opts) do
    default_opts = Map.merge(opts, %{vhost: "/"})
    {args, default_opts}
  end

  def validate([], _) do
    {:validation_failure, :not_enough_args}
  end

  def validate([_|_] = args, _) when length(args) < 3 do
    {:validation_failure, :not_enough_args}
  end

  def validate([_|_] = args, _) when length(args) > 3 do
    {:validation_failure, :too_many_args}
  end

  def validate(_, _), do: :ok

  def run([component_name, name, value], %{node: node_name, vhost: vhost}) do
    node_name
    |> Helpers.parse_node
    |> :rabbit_misc.rpc_call(
      :rabbit_runtime_parameters,
      :parse_set,
      [vhost, component_name, name, value, :none]
    )
  end


  def usage, do: "set_parameter [-p <vhost>] <component_name> <name> <value>"

  def flags, do: @flags

  def banner([component_name, name, value], _), do: "Setting runtime parameter \"#{component_name}\" for component \"#{name}\" to \"#{value}\" ..."
end
