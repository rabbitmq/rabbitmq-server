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
## Copyright (c) 2007-2019 Pivotal Software, Inc.  All rights reserved.

defmodule RabbitMQ.CLI.Ctl.Commands.EvalCommand do
  alias RabbitMQ.CLI.Core.DocGuide

  @behaviour RabbitMQ.CLI.CommandBehaviour

  use RabbitMQ.CLI.Core.MergesNoDefaults

  def validate([], _) do
    {:validation_failure, :not_enough_args}
  end

  def validate(["" | _], _) do
    {:validation_failure, "Expression must not be blank"}
  end

  def validate([expr | _], _) do
    case parse_expr(expr) do
      {:ok, _} -> :ok
      {:error, err} -> {:validation_failure, err}
    end
  end

  def run([expr | arguments], %{node: node_name} = opts) do
    {:ok, parsed} = parse_expr(expr)
    bindings = make_bindings(arguments, opts)

    case :rabbit_misc.rpc_call(node_name, :erl_eval, :exprs, [parsed, bindings]) do
      {:value, value, _} -> {:ok, value}
      err -> err
    end
  end

  use RabbitMQ.CLI.DefaultOutput

  def formatter(), do: RabbitMQ.CLI.Formatters.Erlang

  def usage, do: "eval <expression>"

  def usage_additional() do
    [
      ["<expression>", "Expression to evaluate"]
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

  def description(), do: "Evaluates a snippet of Erlang code on the target node"

  def banner(_, _), do: nil

  #
  # Implementation
  #

  defp make_bindings(arguments, opts) do
    Enum.with_index(arguments, 1)
    |> Enum.map(fn {val, index} -> {String.to_atom("_#{index}"), val} end)
    |> Enum.concat(option_bindings(opts))
  end

  defp option_bindings(opts) do
    Enum.to_list(opts)
    |> Enum.map(fn {key, val} -> {String.to_atom("_#{key}"), val} end)
  end

  defp parse_expr(expr) do
    expr_str = to_charlist(expr)

    case :erl_scan.string(expr_str) do
      {:ok, scanned, _} ->
        case :erl_parse.parse_exprs(scanned) do
          {:ok, parsed} -> {:ok, parsed}
          {:error, err} -> {:error, format_parse_error(err)}
        end

      {:error, err, _} ->
        {:error, format_parse_error(err)}
    end
  end

  defp format_parse_error({_line, mod, err}) do
    to_string(:lists.flatten(mod.format_error(err)))
  end
end
