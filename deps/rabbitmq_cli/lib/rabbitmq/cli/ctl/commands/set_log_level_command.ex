## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.

defmodule RabbitMQ.CLI.Ctl.Commands.SetLogLevelCommand do
  alias RabbitMQ.CLI.Core.DocGuide
  @behaviour RabbitMQ.CLI.CommandBehaviour
  @known_levels [
    "debug",
    "info",
    "notice",
    "warning",
    "error",
    "critical",
    "alert",
    "emergency",
    "none"
  ]

  use RabbitMQ.CLI.Core.MergesNoDefaults

  def validate([], _) do
    {:validation_failure, :not_enough_args}
  end
  def validate(args, _) when length(args) > 1 do
    {:validation_failure, :too_many_args}
  end

  def validate([level], _) do
    case Enum.member?(@known_levels, level) do
      true ->
        :ok

      false ->
        {:error, "level #{level} is not supported. Try one of debug, info, warning, error, none"}
    end
  end

  use RabbitMQ.CLI.Core.RequiresRabbitAppRunning

  def run([log_level], %{node: node_name}) do
    arg = String.to_atom(log_level)
    :rabbit_misc.rpc_call(node_name, :rabbit, :set_log_level, [arg])
  end

  def usage, do: "set_log_level <log_level>"

  def usage_additional() do
    [
      ["<log_level>", "new log level"]
    ]
  end

  def usage_doc_guides() do
    [
      DocGuide.logging()
    ]
  end

  def help_section(), do: :configuration

  def description(), do: "Sets log level in the running node"

  def banner([log_level], _), do: "Setting log level to \"#{log_level}\" ..."

  def output({:error, {:invalid_log_level, level}}, _opts) do
    {:error, RabbitMQ.CLI.Core.ExitCodes.exit_software(),
     "level #{level} is not supported. Try one of debug, info, warning, error, none"}
  end

  use RabbitMQ.CLI.DefaultOutput
end
