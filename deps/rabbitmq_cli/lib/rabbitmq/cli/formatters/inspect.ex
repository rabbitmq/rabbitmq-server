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
