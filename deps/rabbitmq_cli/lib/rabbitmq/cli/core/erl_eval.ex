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

defmodule RabbitMQ.CLI.Core.ErlEval do
  def parse_expr(expr) do
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
