## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.

defmodule RabbitMQ.CLI.Ctl.Commands.SetChannelInterceptorPrioritiesCommand do
  @behaviour RabbitMQ.CLI.CommandBehaviour

  use RabbitMQ.CLI.Core.AcceptsDefaultSwitchesAndTimeout
  use RabbitMQ.CLI.Core.MergesNoDefaults

  def validate([], _) do
    {:validation_failure, :not_enough_args}
  end

  def validate(args, _) when rem(length(args), 2) != 0 do
    {:validation_failure,
     {:bad_argument,
      "Interceptors and priorities must be provided as <interceptor> <priority> pairs"}}
  end

  def validate(args, _) do
    priorities = args |> Enum.drop(1) |> Enum.take_every(2)

    case Enum.find(priorities, fn p -> not integer_string?(p) end) do
      nil ->
        :ok

      invalid ->
        {:validation_failure,
         {:bad_argument, "Priority \"#{invalid}\" is not a valid integer"}}
    end
  end

  use RabbitMQ.CLI.Core.RequiresRabbitAppRunning

  def run(args, %{node: node_name, timeout: timeout}) do
    priorities =
      args
      |> Enum.chunk_every(2)
      |> Enum.map(fn [interceptor, priority] ->
        {interceptor, String.to_integer(priority)}
      end)

    :rabbit_misc.rpc_call(
      node_name,
      :rabbit_channel_interceptor,
      :set_priorities,
      [priorities],
      timeout
    )
  end

  use RabbitMQ.CLI.DefaultOutput

  def usage,
    do:
      "set_channel_interceptor_priorities <interceptor> <priority> [<interceptor> <priority> ...]"

  def usage_additional() do
    [
      ["<interceptor>", "Channel interceptor module name"],
      ["<priority>", "Priority (integer) the interceptor runs at, lower runs first"]
    ]
  end

  def help_section(), do: :configuration

  def description(),
    do:
      "Sets the priorities of the given channel interceptors on the running node. " <>
        "The change applies immediately but is not persisted and does not survive a node restart"

  def banner(args, _) do
    pairs =
      args
      |> Enum.chunk_every(2)
      |> Enum.map_join(", ", fn [interceptor, priority] -> "#{interceptor}=#{priority}" end)

    "Setting channel interceptor priorities to [#{pairs}] ..."
  end

  defp integer_string?(value) do
    case Integer.parse(value) do
      {_, ""} -> true
      _ -> false
    end
  end
end
