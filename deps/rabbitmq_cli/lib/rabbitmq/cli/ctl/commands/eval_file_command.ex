## The contents of this file are subject to the Mozilla Public License
## Version 1.1 (the "License"); you may not use this file except in
## compliance with the License. You may obtain a copy of the License
## at https://www.mozilla.org/MPL/
##
## Software distributed under the License is distributed on an "AS IS"
## basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
## the License for the specific language governing rights and
## limitations under the License.
##
## The Original Code is RabbitMQ.
##
## The Initial Developer of the Original Code is GoPivotal, Inc.
## Copyright (c) 2007-2020 VMware, Inc. or its affiliates.  All rights reserved.

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
