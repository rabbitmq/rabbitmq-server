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

  def set_vm_memory_high_watermark([arg], %{node: node_name}) when is_number(arg) and arg >= 0.0 do
    node_name
    |> Helpers.parse_node
    |> :rabbit_misc.rpc_call(
      :vm_memory_monitor,
      :set_vm_memory_high_watermark,
      [arg]
    )
  end

  def set_vm_memory_high_watermark([arg], _) when is_number(arg) and arg < 0.0 do
    {:bad_argument, arg}
  end

  def set_vm_memory_high_watermark([arg], %{} = opts) when is_binary(arg) do
    case Float.parse(arg) do
      :error    -> {:bad_argument, [arg]}
      {num, _}  -> set_vm_memory_high_watermark([num], opts)
    end
  end

  def set_vm_memory_high_watermark([], _) do
    HelpCommand.help
    {:bad_argument, []}
  end

  def set_vm_memory_high_watermark([_|_], _) do
    HelpCommand.help
    {:bad_argument, ["too many arguments"]}
  end

  def usage, do: "set_vm_memory_high_watermark <fraction>"
end
