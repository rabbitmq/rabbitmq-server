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

  @behaviour CommandBehaviour
  @flags [:param]

  def run([], _) do
    {:not_enough_args, []}
  end

  def run([_|_] = args, _) when length(args) < 3 do
    {:not_enough_args, args}
  end

  def run([_|_] = args, _) when length(args) > 3 do
    {:too_many_args, args}
  end

  def run([component_name, name, value] = args, %{node: node_name, param: vhost} = opts) do
    info(args, opts)
    node_name
    |> Helpers.parse_node
    |> :rabbit_misc.rpc_call(
      :rabbit_runtime_parameters,
      :parse_set,
      [vhost, component_name, name, value, :none]
    )
  end

  def run([_, _, _] = args, %{node: _} = opts) do
    default_opts = Map.merge(opts, %{param: "/"})
    run(args, default_opts)
  end

  def usage, do: "set_parameter [-p <vhost>] <component_name> <name> <value>"

  def flags, do: @flags

  defp info(_, %{quiet: true}), do: nil
  defp info([component_name, name, value], _), do: IO.puts "Setting runtime parameter \"#{component_name}\" for component \"#{name}\" to \"#{value}\" ..."
end
