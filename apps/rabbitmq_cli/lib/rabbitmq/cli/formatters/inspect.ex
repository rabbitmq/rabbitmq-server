## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.
alias RabbitMQ.CLI.Formatters.FormatterHelpers

defmodule RabbitMQ.CLI.Formatters.Inspect do
  @behaviour RabbitMQ.CLI.FormatterBehaviour

  def format_output(output, _) do
    case is_binary(output) do
      true -> output
      false -> inspect(output)
    end
  end

  def format_stream(stream, options) do
    elements =
      Stream.scan(
        stream,
        :empty,
        FormatterHelpers.without_errors_2(fn element, previous ->
          separator =
            case previous do
              :empty -> ""
              _ -> ","
            end

          format_element(element, separator, options)
        end)
      )

    Stream.concat([["["], elements, ["]"]])
  end

  def format_element(val, separator, options) do
    separator <> format_output(val, options)
  end
end
