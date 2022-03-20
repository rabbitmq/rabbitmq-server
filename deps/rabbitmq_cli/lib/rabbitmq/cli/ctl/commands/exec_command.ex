## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.

defmodule RabbitMQ.CLI.Ctl.Commands.ExecCommand do
  alias RabbitMQ.CLI.Core.DocGuide

  @behaviour RabbitMQ.CLI.CommandBehaviour

  use RabbitMQ.CLI.Core.MergesNoDefaults

  def switches(), do: [offline: :boolean]

  def distribution(%{offline: true}), do: :none
  def distribution(%{}), do: :cli

  def validate([], _) do
    {:validation_failure, :not_enough_args}
  end

  def validate(args, _) when length(args) > 1 do
    {:validation_failure, :too_many_args}
  end

  def validate([""], _) do
    {:validation_failure, "Expression must not be blank"}
  end

  def validate([string], _) do
    try do
      Code.compile_string(string)
      :ok
    rescue
      ex in SyntaxError ->
        {:validation_failure, "SyntaxError: " <> Exception.message(ex)}

      _ ->
        :ok
    end
  end

  def run([expr], %{} = opts) do
    try do
      {val, _} = Code.eval_string(expr, [options: opts], __ENV__)
      {:ok, val}
    rescue
      ex ->
        {:error, Exception.message(ex)}
    end
  end

  use RabbitMQ.CLI.DefaultOutput

  def formatter(), do: RabbitMQ.CLI.Formatters.Inspect

  def banner(_, _), do: nil

  def usage, do: "exec <expression> [--offline]"

  def usage_additional() do
    [
      ["<expression>", "Expression to evaluate"],
      ["--offline", "disable inter-node communication"]
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

  def description(), do: "Evaluates a snippet of Elixir code on the CLI node"
end
