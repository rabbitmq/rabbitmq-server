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


defmodule RabbitMQ.CLI.Ctl.Commands.EvalCommand do
  @behaviour RabbitMQ.CLI.CommandBehaviour
  use RabbitMQ.CLI.DefaultOutput

  def merge_defaults(args, opts), do: {args, opts}

  def switches(), do: []
  def aliases(), do: []

  def formatter(), do: RabbitMQ.CLI.Formatters.ErlTerms

  def validate([], _) do
    {:validation_failure, :not_enough_args}
  end

  def validate(args, _) when length(args) > 1 do
    {:validation_failure, :too_many_args}
  end

  def validate([""], _) do
    {:validation_failure, "Expression must not be blank"}
  end

  def validate([expr], _) do
    case parse_expr(expr) do
      {:ok, _}      -> :ok;
      {:error, err} -> {:validation_failure, err}
    end
  end

  def validate([_,_], _), do: :ok

  def run([expr],  %{node: node_name}) do
    {:ok, parsed} = parse_expr(expr)
    case :rabbit_misc.rpc_call(node_name, :erl_eval, :exprs, [parsed, []]) do
      {:value, value, _} -> {:ok, value};
      err                -> err
    end
  end

  def usage, do: "eval <expr>"

  def banner(_, _), do: nil

  def flags(), do: []

  defp parse_expr(expr) do
    expr_str = to_char_list(expr)
    case :erl_scan.string(expr_str) do
      {:ok, scanned, _} ->
        case :erl_parse.parse_exprs(scanned) do
          {:ok, parsed} -> {:ok, parsed};
          {:error, err} -> {:error, format_parse_error(err)}
        end;
      {:error, err, _}  ->
        {:error, format_parse_error(err)}
    end
  end

  defp format_parse_error({_line, mod, err}) do
    to_string(:lists.flatten(mod.format_error(err)))
  end

end
