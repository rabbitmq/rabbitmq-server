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


defmodule RabbitMQ.CLI.Ctl.Commands.SetVmMemoryHighWatermarkCommand do
  alias RabbitMQ.CLI.Core.Helpers

  @behaviour RabbitMQ.CLI.CommandBehaviour
  use RabbitMQ.CLI.DefaultOutput

  def merge_defaults(args, opts), do: {args, opts}

  def validate([], _) do
    {:validation_failure, :not_enough_args}
  end

  def validate(["absolute"], _) do
    {:validation_failure, :not_enough_args}
  end

  def validate(["absolute"|_] = args, _) when length(args) > 2 do
    {:validation_failure, :too_many_args}
  end

  def validate(["absolute", arg], _) do
    case Integer.parse(arg) do
      :error        ->  {:validation_failure, :bad_argument}
      {_, rest} ->
        case Enum.member?(Helpers.memory_units, rest) do
          true -> :ok
          false -> case Float.parse(arg) do
                     {_, orest} when orest == rest ->
                       {:validation_failure, {:bad_argument, "Invalid units."}}
                     _ ->
                       {:validation_failure, {:bad_argument, "The threshold should be an integer."}}
                   end
        end
    end
  end

  def validate([_|_] = args, _) when length(args) > 1 do
    {:validation_failure, :too_many_args}
  end

  def validate([arg], _) when is_number(arg) and (arg < 0.0 or arg > 1.0) do
    {:validation_failure, {:bad_argument, "The threshold should be a fraction between 0.0 and 1.0"}}
  end
  def validate([arg], %{}) when is_binary(arg) do
    case Float.parse(arg) do
      {arg, ""} when is_number(arg) and (arg < 0.0 or arg > 1.0) ->
               {:validation_failure, {:bad_argument, "The threshold should be a fraction between 0.0 and 1.0"}}
      {_, ""}   ->  :ok
      _           ->  {:validation_failure, :bad_argument}
    end
  end
  def validate(_, _), do: :ok

  use RabbitMQ.CLI.Core.RequiresRabbitAppRunning

  def run(["absolute", arg], opts) do
    case Integer.parse(arg) do
      {num, rest}   ->  valid_units = rest in Helpers.memory_units
                        set_vm_memory_high_watermark_absolute({num, rest}, valid_units, opts)
    end
  end

  def run([arg], %{node: node_name}) when is_number(arg) and arg >= 0.0 do
    :rabbit_misc.rpc_call(node_name,
      :vm_memory_monitor,
      :set_vm_memory_high_watermark,
      [arg]
    )
  end

  def run([arg], %{} = opts) when is_binary(arg) do
    case Float.parse(arg) do
      {num, ""}   ->  run([num], opts)
    end
  end

  defp set_vm_memory_high_watermark_absolute({num, rest}, true, %{node: node_name}) when num > 0 do
      val = Helpers.memory_unit_absolute(num, rest)
      :rabbit_misc.rpc_call(node_name,
        :vm_memory_monitor,
        :set_vm_memory_high_watermark,
        [{:absolute, val}])
  end

  def usage, do: ["set_vm_memory_high_watermark <fraction>", "set_vm_memory_high_watermark absolute <value>"]

  def banner(["absolute", arg], %{node: node_name}), do: "Setting memory threshold on #{node_name} to #{arg} bytes ..."
  def banner([arg], %{node: node_name}), do: "Setting memory threshold on #{node_name} to #{arg} ..."
end
