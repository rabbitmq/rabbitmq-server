## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.

defmodule RabbitMQ.CLI.Formatters.StringPerLine do
  @doc """
  Use this to output one stream (collection) element per line,
  using the string formatter. Flattens the stream.
  """

  alias RabbitMQ.CLI.Formatters.FormatterHelpers
  alias RabbitMQ.CLI.Core.Helpers
  import RabbitMQ.CLI.Core.Platform, only: [line_separator: 0]

  @behaviour RabbitMQ.CLI.FormatterBehaviour

  def format_output(output, _) do
    Enum.map(output, fn el -> Helpers.string_or_inspect(el) end)
  end

  def format_stream(stream, options) do
    Stream.scan(
      stream,
      :empty,
      FormatterHelpers.without_errors_2(fn element, previous ->
        separator =
          case previous do
            :empty -> ""
            _ -> line_separator()
          end

        format_element(element, separator, options)
      end)
    )
  end

  def format_element(val, separator, options) do
    separator <> format_output(val, options)
  end
end
