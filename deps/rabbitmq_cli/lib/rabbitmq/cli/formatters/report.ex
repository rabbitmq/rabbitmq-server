## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2017-2022 VMware, Inc. or its affiliates.  All rights reserved.

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
