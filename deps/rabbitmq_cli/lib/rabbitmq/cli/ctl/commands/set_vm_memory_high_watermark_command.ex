## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.

defmodule RabbitMQ.CLI.Ctl.Commands.SetVmMemoryHighWatermarkCommand do
  alias RabbitMQ.CLI.Core.DocGuide
  import RabbitMQ.CLI.Core.Memory

  @behaviour RabbitMQ.CLI.CommandBehaviour

  use RabbitMQ.CLI.Core.MergesNoDefaults

  def validate([], _) do
    {:validation_failure, :not_enough_args}
  end

  def validate(["absolute"], _) do
    {:validation_failure, :not_enough_args}
  end

  def validate(["absolute" | _] = args, _) when length(args) > 2 do
    {:validation_failure, :too_many_args}
  end

  def validate(["absolute", arg], _) do
    case Integer.parse(arg) do
      :error ->
        {:validation_failure, :bad_argument}

      {_, rest} ->
        case Enum.member?(memory_units(), rest) do
          true ->
            :ok

          false ->
            case Float.parse(arg) do
              {_, orest} when orest == rest ->
                {:validation_failure, {:bad_argument, "Invalid units."}}

              _ ->
                {:validation_failure, {:bad_argument, "The threshold should be an integer."}}
            end
        end
    end
  end

  def validate([_ | _] = args, _) when length(args) > 1 do
    {:validation_failure, :too_many_args}
  end

  def validate([arg], _) when is_number(arg) and (arg < 0.0 or arg > 1.0) do
    {:validation_failure,
     {:bad_argument, "The threshold should be a fraction between 0.0 and 1.0"}}
  end

  def validate([arg], %{}) when is_binary(arg) do
    case Float.parse(arg) do
      {arg, ""} when is_number(arg) and (arg < 0.0 or arg > 1.0) ->
        {:validation_failure,
         {:bad_argument, "The threshold should be a fraction between 0.0 and 1.0"}}

      {_, ""} ->
        :ok

      _ ->
        {:validation_failure, :bad_argument}
    end
  end

  def validate(_, _), do: :ok

  use RabbitMQ.CLI.Core.RequiresRabbitAppRunning

  def run(["absolute", arg], opts) do
    case Integer.parse(arg) do
      {num, rest} ->
        valid_units = rest in memory_units()
        set_vm_memory_high_watermark_absolute({num, rest}, valid_units, opts)
    end
  end

  def run([arg], %{node: node_name}) when is_number(arg) and arg >= 0.0 do
    :rabbit_misc.rpc_call(
      node_name,
      :vm_memory_monitor,
      :set_vm_memory_high_watermark,
      [arg]
    )
  end

  def run([arg], %{} = opts) when is_binary(arg) do
    case Float.parse(arg) do
      {num, ""} -> run([num], opts)
    end
  end

  use RabbitMQ.CLI.DefaultOutput

  def usage do
    "set_vm_memory_high_watermark <fraction> | absolute <value>"
  end

  def usage_additional() do
    [
      ["<fraction>", "New limit as a fraction of total memory reported by the OS"],
      ["absolute <value>", "New limit as an absolute value with units, e.g. 1GB"]
    ]
  end

  def usage_doc_guides() do
    [
      DocGuide.alarms(),
      DocGuide.memory_use(),
    ]
  end

  def help_section(), do: :configuration

  def description(), do: "Sets the vm_memory_high_watermark setting"

  def banner(["absolute", arg], %{node: node_name}) do
    "Setting memory threshold on #{node_name} to #{arg} bytes ..."
  end

  def banner([arg], %{node: node_name}) do
    "Setting memory threshold on #{node_name} to #{arg} ..."
  end

  #
  # Implementation
  #

  defp set_vm_memory_high_watermark_absolute({num, rest}, true, %{node: node_name})
       when num > 0 do
    val = memory_unit_absolute(num, rest)

    :rabbit_misc.rpc_call(
      node_name,
      :vm_memory_monitor,
      :set_vm_memory_high_watermark,
      [{:absolute, val}]
    )
  end
end
