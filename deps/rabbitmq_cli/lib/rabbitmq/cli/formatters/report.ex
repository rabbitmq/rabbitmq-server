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
## Copyright (c) 2017 Pivotal Software, Inc.  All rights reserved.

defmodule RabbitMQ.CLI.Formatters.Report do
  alias RabbitMQ.CLI.Formatters.FormatterHelpers
  alias RabbitMQ.CLI.Core.{Output, Config}

  @behaviour RabbitMQ.CLI.FormatterBehaviour
  def format_output(_, _) do
    raise "format_output is not implemented for report formatter"
  end

  def format_stream(stream, options) do
    quiet = options[:quiet] || options[:silent] || false

    Stream.flat_map(
      stream,
      FormatterHelpers.without_errors_1(fn
        {_command, _banner, {:error, _} = err} ->
          err

        {_command, _banner, {:error, _, _} = err} ->
          err

        {command, banner, result} ->
          case quiet do
            true ->
              Stream.concat([""], format_result(command, result, options))

            false ->
              Stream.concat(["" | banner_list(banner)], format_result(command, result, options))
          end
      end)
    )
  end

  def format_result(command, output, options) do
    formatter = Config.get_formatter(command, options)

    case Output.format_output(output, formatter, options) do
      :ok -> []
      {:ok, val} -> [val]
      {:stream, stream} -> stream
    end
  end

  def banner_list([_ | _] = list), do: list
  def banner_list(val), do: [val]
end
