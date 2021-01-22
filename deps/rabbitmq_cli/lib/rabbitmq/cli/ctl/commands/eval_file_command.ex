## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.

defmodule RabbitMQ.CLI.Ctl.Commands.EvalFileCommand do
  alias RabbitMQ.CLI.Core.{DocGuide, ErlEval, ExitCodes}
  alias RabbitMQ.CLI.Ctl.Commands.EvalCommand

  @behaviour RabbitMQ.CLI.CommandBehaviour

  use RabbitMQ.CLI.Core.MergesNoDefaults

  def validate([], _) do
    {:validation_failure, :not_enough_args}
  end

  def validate(["" | _], _) do
    {:validation_failure, "File path must not be blank"}
  end

  def validate([file_path], _) do
    case File.read(file_path) do
      {:ok, expr} ->
        case ErlEval.parse_expr(expr) do
          {:ok, _}      -> :ok
          {:error, err} -> {:validation_failure, err}
        end
      {:error, :enoent} ->
        {:validation_failure, "File #{file_path} does not exist"}
      {:error, :eacces} ->
        {:validation_failure, "Insufficient permissions to read file #{file_path}"}
      {:error, err} ->
        {:validation_failure, err}
    end
  end

  def run([file_path], opts) do
    case File.read(file_path) do
      {:ok, expr}   -> EvalCommand.run([expr], opts)
      {:error, err} -> {:error, err}
    end

  end

  def output({:error, msg}, _) do
    {:error, ExitCodes.exit_dataerr(), "Evaluation failed: #{msg}"}
  end
  use RabbitMQ.CLI.DefaultOutput

  def formatter(), do: RabbitMQ.CLI.Formatters.Erlang

  def usage, do: "eval_file <file path>"

  def usage_additional() do
    [
      ["<file path>", "Path to the file to evaluate"]
    ]
  end

  def usage_doc_guides() do
    [
      DocGuide.cli(),
      DocGuide.monitoring(),
      DocGuide.troubleshooting()
    ]
  end

  def help_section(), do: :operations

  def description(), do: "Evaluates a file that contains a snippet of Erlang code on the target node"

  def banner(_, _), do: nil
end
