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


defmodule TraceOffCommand do
  @default_vhost "/"

  def trace_off([_|_] = args, _), do: {:too_many_args, args}
  def trace_off([], %{node: node_name, param: vhost} = opts) do
    info(opts)
    node_name
    |> Helpers.parse_node
    |> :rabbit_misc.rpc_call(:rabbit_trace, :stop, [vhost])
  end

  def trace_off([], %{node: _} = opts) do
    trace_off([], Map.merge(opts, %{param: @default_vhost}))
  end

  def usage, do: "trace_off [-p <vhost>]"

  defp info(%{quiet: true}), do: nil
  defp info(%{param: vhost}), do: IO.puts "Stopping tracing for vhost \"#{vhost}\" ..."
end
