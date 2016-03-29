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


defmodule SetDiskFreeLimitCommand do

  def set_disk_free_limit([], _) do
    HelpCommand.help
    {:bad_argument, []}
  end

  def set_disk_free_limit([_|rest], _) when length(rest) > 0 do
    HelpCommand.help
    {:bad_argument, []}
  end

  def set_disk_free_limit([limit], %{node: node_name}) when is_integer(limit) do
    node_name
    |> Helpers.parse_node
    |> :rabbit_misc.rpc_call(:rabbit_disk_monitor, :set_disk_free_limit, [limit])
  end

  def set_disk_free_limit([limit], %{node: _} = opts) when is_float(limit) do
    set_disk_free_limit([limit |> Float.floor |> round], opts)
  end

  def set_disk_free_limit([limit], %{node: _} = opts) do
    case Integer.parse(limit) do
      {limit_val, ""} -> set_disk_free_limit([limit_val], opts)
      _               -> {:bad_argument, [limit]}
    end
  end

  def usage, do: "set_disk_free_limit <disk_limit>"
end
