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
## Copyright (c) 2007-2017 Pivotal Software, Inc.  All rights reserved.

alias RabbitMQ.CLI.Formatters.FormatterHelpers, as: FormatterHelpers

defmodule RabbitMQ.CLI.Formatters.Table do

  @behaviour RabbitMQ.CLI.FormatterBehaviour

  def format_stream(stream, options) do
    ## Flatten list_consumers
    Stream.flat_map(stream,
                    fn([first | _] = element) ->
                        case Keyword.keyword?(first) or is_map(first) do
                          true  -> element;
                          false -> [element]
                        end
                      (other) ->
                        [other]
                    end)
    |> Stream.map(FormatterHelpers.without_errors_1(
                    fn(element) ->
                      format_output(element, options)
                    end))
  end

  def format_output(output, options) when is_map(output) do
    escaped = escaped?(options)
    format_line(output, escaped)
  end
  def format_output([], _) do
    ""
  end
  def format_output(output, options)do
    escaped = escaped?(options)
    case Keyword.keyword?(output) do
        true  -> format_line(output, escaped);
        false -> format_inspect(output)
    end
  end

  defp escaped?(_), do: true

  defp format_line(line, escaped) do
    values = Enum.map(line,
                      fn({_k, v}) ->
                        FormatterHelpers.format_info_item(v, escaped)
                      end)
    Enum.join(values, "\t")
  end

  defp format_inspect(output) do
    case is_binary(output) do
      true  -> output;
      false -> inspect(output)
    end
  end
end
