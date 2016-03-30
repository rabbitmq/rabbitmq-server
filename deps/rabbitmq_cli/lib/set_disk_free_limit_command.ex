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

  import Helpers, only: [memory_unit_absolute: 2]

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

  def set_disk_free_limit([limit], %{node: _} = opts) when is_binary(limit) do
    case Integer.parse(limit) do
      {limit_val, ""}     -> set_disk_free_limit([limit_val], opts)
      {limit_val, units}  -> set_disk_free_limit_in_units([limit_val, units], opts)
      _                   -> {:bad_argument, limit}
    end
  end

  defp set_disk_free_limit_in_units([limit_val, units], opts) do
    case memory_unit_absolute(limit_val, units) do
      scaled_limit when is_integer(scaled_limit) -> set_disk_free_limit([scaled_limit], opts)
      _     -> {:bad_argument, ["#{limit_val}#{units}"]}
    end
  end

  def usage, do: "set_disk_free_limit <disk_limit>"
end
