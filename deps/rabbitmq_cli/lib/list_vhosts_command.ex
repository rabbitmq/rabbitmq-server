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


defmodule ListVhostsCommand do

  def list_vhosts([], opts) do
    list_vhosts(["name"], opts)
  end

  def list_vhosts([_,_|rest] = args, _) when length(rest) != 0 do
    {:too_many_args, args}
  end

  def list_vhosts([_|_] = args, %{node: node_name, timeout: time_out}) do
    node_name
    |> Helpers.parse_node
    |> :rabbit_misc.rpc_call(:rabbit_vhost, :info_all, [], time_out)
    |> filter_by_arg(args)
  end

  def usage, do: "list_vhosts [<vhostinfoitem> ...]"

  defp filter_by_arg(vhosts, _) when is_tuple(vhosts) do
    vhosts
  end

  defp filter_by_arg(vhosts, [arg]) do
    case valid_arg(arg) do
      false -> {:error, {:bad_info_key, arg}}
      true  -> 
        symbol_arg = String.to_atom(arg)
        Enum.map(vhosts, fn(vhost) -> [{symbol_arg, vhost[symbol_arg]}] end)
    end
  end

  defp filter_by_arg(vhosts, [arg1, arg2] = args) do
    case {valid_arg(arg1), valid_arg(arg2)} do
      {false, _}  -> {:error, {:bad_info_key, arg1}}
      {_, false}  -> {:error, {:bad_info_key, arg2}}
      _           -> 
        symbol_args = args |> Enum.map(&(String.to_atom(&1))) |> Enum.uniq
        vhosts
        |> Enum.map(
          fn(vhost) ->
            Enum.filter_map(
              symbol_args,
              fn(arg) -> vhost[arg] != nil end,
              fn(arg) -> {arg, vhost[arg]} end
            )
          end
        )
    end
  end

  defp valid_arg("name"), do: true
  defp valid_arg("tracing"), do: true
  defp valid_arg(_), do: false
end
