## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.

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
