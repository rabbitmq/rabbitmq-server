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


defmodule SetVmMemoryHighWatermarkCommand do

  def set_vm_memory_high_watermark([], _) do
    HelpCommand.help
    {:bad_argument, []}
  end

  def set_vm_memory_high_watermark(["absolute"], _) do
    HelpCommand.help
    {:bad_argument, []}
  end

  def set_vm_memory_high_watermark(["absolute"|[arg]], opts) do
    case Integer.parse(arg) do
      :error    -> {:bad_argument, [arg]}
      {num, rest}  ->
          valid_units = rest in Helpers.memory_units
          set_vm_memory_high_watermark_absolute({num, rest}, valid_units, opts)
    end
  end

  def set_vm_memory_high_watermark([arg], _) when is_number(arg) and (arg < 0.0 or arg > 1.0) do
    {:bad_argument, [arg]}
  end

  def set_vm_memory_high_watermark([arg], %{node: node_name}) when is_number(arg) and arg >= 0.0 do
    node_name
    |> Helpers.parse_node
    |> :rabbit_misc.rpc_call(
      :vm_memory_monitor,
      :set_vm_memory_high_watermark,
      [arg]
    )
  end

  def set_vm_memory_high_watermark([arg], %{} = opts) when is_binary(arg) do
    case Float.parse(arg) do
      {num, ""}   -> set_vm_memory_high_watermark([num], opts)
      _           -> {:bad_argument, [arg]}
    end
  end

  def set_vm_memory_high_watermark([_|rest], _) when rest > 0 do
    HelpCommand.help
    {:bad_argument, ["too many arguments"]}
  end

  defp set_vm_memory_high_watermark_absolute({num, rest}, true, %{node: node_name}) when num > 0 do
      val = Helpers.memory_unit_absolute(num, rest)
      node_name
      |> Helpers.parse_node
      |> :rabbit_misc.rpc_call(
        :vm_memory_monitor,
        :set_vm_memory_high_watermark,
        [{:absolute, val}])
  end

  defp set_vm_memory_high_watermark_absolute({num, rest}, _, _) when num < 0 do
    {:bad_argument, ["#{num}#{rest}"]}
  end

  defp set_vm_memory_high_watermark_absolute({num, rest}, false, _) do
    {:bad_argument, ["#{num}#{rest}"]}
  end

  def usage, do: ["set_vm_memory_high_watermark <fraction>", "set_vm_memory_high_watermark absolute <value>"]
end
