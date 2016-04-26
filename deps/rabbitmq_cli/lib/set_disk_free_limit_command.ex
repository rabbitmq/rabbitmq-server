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
    {:not_enough_args, []}
  end

  ## ----------------------- Memory-Relative Call ----------------------------

  def set_disk_free_limit(["mem_relative"], _) do
    {:not_enough_args, ["mem_relative"]}
  end

  def set_disk_free_limit(["mem_relative", _ | rest] = args, _) when length(rest) > 0 do
    {:too_many_args, args}
  end

  def set_disk_free_limit(["mem_relative", fraction], _)
      when is_float(fraction) and fraction < 0.0 do
    {:bad_argument, fraction}
  end

  def set_disk_free_limit(["mem_relative", fraction], %{node: node_name})
      when is_float(fraction) do
    make_rpc_call(node_name, [{:mem_relative, fraction}])
  end

  def set_disk_free_limit(["mem_relative", integer_input], %{node: node_name})
      when is_integer(integer_input) do
    make_rpc_call(node_name, [{:mem_relative, integer_input * 1.0}])
  end

  def set_disk_free_limit(["mem_relative", fraction_str], %{node: _} = opts) when is_binary(fraction_str) do
    case Float.parse(fraction_str) do
      {fraction_val, ""}  -> set_disk_free_limit(["mem_relative", fraction_val], opts)
      _                   -> {:bad_argument, [fraction_str]}
    end
  end

  ## ------------------------ Absolute Size Call -----------------------------

  # Has to come after mem_relative calls for pattern-matching
  def set_disk_free_limit([_|rest] = args, _) when length(rest) > 0 do
    {:too_many_args, args}
  end

  def set_disk_free_limit([limit], %{node: node_name}) when is_integer(limit) do
    make_rpc_call(node_name, [limit])
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

  ## ------------------------- Helpers / Misc --------------------------------

  defp make_rpc_call(node_name, args) do
    node_name
    |> Helpers.parse_node
    |> :rabbit_misc.rpc_call(:rabbit_disk_monitor, :set_disk_free_limit, args)
  end

  def usage, do: "set_disk_free_limit <disk_limit>"
end
