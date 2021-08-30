## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.

defmodule RabbitMQ.CLI.Ctl.Commands.SetDiskFreeLimitCommand do
  alias RabbitMQ.CLI.Core.DocGuide
  import RabbitMQ.CLI.Core.Memory

  @behaviour RabbitMQ.CLI.CommandBehaviour

  use RabbitMQ.CLI.Core.MergesNoDefaults

  def validate([], _) do
    {:validation_failure, :not_enough_args}
  end

  def validate(["mem_relative"], _) do
    {:validation_failure, :not_enough_args}
  end

  def validate(["mem_relative" | _] = args, _) when length(args) != 2 do
    {:validation_failure, :too_many_args}
  end

  def validate([limit], _) do
    case Integer.parse(limit) do
      {_, ""} ->
        :ok

      {limit_val, units} ->
        case memory_unit_absolute(limit_val, units) do
          scaled_limit when is_integer(scaled_limit) -> :ok
          _ -> {:validation_failure, :bad_argument}
        end

      _ ->
        {:validation_failure, :bad_argument}
    end
  end

  def validate(["mem_relative", fraction], _) do
    case Float.parse(fraction) do
      {val, ""} when val >= 0.0 -> :ok
      _ -> {:validation_failure, :bad_argument}
    end
  end

  def validate([_ | rest], _) when length(rest) > 0 do
    {:validation_failure, :too_many_args}
  end

  use RabbitMQ.CLI.Core.RequiresRabbitAppRunning

  def run(["mem_relative", _] = args, opts) do
    set_disk_free_limit_relative(args, opts)
  end

  def run([limit], %{node: _} = opts) when is_binary(limit) do
    case Integer.parse(limit) do
      {limit_val, ""} -> set_disk_free_limit_absolute([limit_val], opts)
      {limit_val, units} -> set_disk_free_limit_in_units([limit_val, units], opts)
    end
  end

  def run([limit], opts) do
    set_disk_free_limit_absolute([limit], opts)
  end

  use RabbitMQ.CLI.DefaultOutput

  def banner(["mem_relative", arg], %{node: node_name}) do
    "Setting disk free limit on #{node_name} to #{arg} times the total RAM ..."
  end

  def banner([arg], %{node: node_name}),
    do: "Setting disk free limit on #{node_name} to #{arg} bytes ..."

  def usage, do: "set_disk_free_limit <disk_limit> | mem_relative <fraction>"

  def usage_additional() do
    [
      ["<disk_limit>", "New limit as an absolute value with units, e.g. 1GB"],
      ["mem_relative <fraction>", "New limit as a fraction of total memory reported by the OS"]
    ]
  end

  def usage_doc_guides() do
    [
      DocGuide.disk_alarms(),
      DocGuide.alarms()
    ]
  end

  def help_section(), do: :configuration

  def description(), do: "Sets the disk_free_limit setting"

  #
  # Implementation
  #

  defp set_disk_free_limit_relative(["mem_relative", fraction], %{node: node_name})
       when is_float(fraction) do
    make_rpc_call(node_name, [{:mem_relative, fraction}])
  end

  defp set_disk_free_limit_relative(["mem_relative", integer_input], %{node: node_name})
       when is_integer(integer_input) do
    make_rpc_call(node_name, [{:mem_relative, integer_input * 1.0}])
  end

  defp set_disk_free_limit_relative(["mem_relative", fraction_str], %{node: _} = opts)
       when is_binary(fraction_str) do
    {fraction_val, ""} = Float.parse(fraction_str)
    set_disk_free_limit_relative(["mem_relative", fraction_val], opts)
  end

  ## ------------------------ Absolute Size Call -----------------------------

  defp set_disk_free_limit_absolute([limit], %{node: node_name}) when is_integer(limit) do
    make_rpc_call(node_name, [limit])
  end

  defp set_disk_free_limit_absolute([limit], %{node: _} = opts) when is_float(limit) do
    set_disk_free_limit_absolute([limit |> Float.floor() |> round], opts)
  end

  defp set_disk_free_limit_in_units([limit_val, units], opts) do
    case memory_unit_absolute(limit_val, units) do
      scaled_limit when is_integer(scaled_limit) ->
        set_disk_free_limit_absolute([scaled_limit], opts)
    end
  end

  defp make_rpc_call(node_name, args) do
    :rabbit_misc.rpc_call(node_name, :rabbit_disk_monitor, :set_disk_free_limit, args)
  end
end
