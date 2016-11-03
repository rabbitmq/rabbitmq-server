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

defmodule RabbitMQ.CLI.Formatters.Table do
  @behaviour RabbitMQ.CLI.Formatters.FormatterBehaviour

  def format_output(output, _options) when is_map(output) do
    format_line(output)
  end
  def format_output(output, _options) do
    case Keyword.keyword?(output) do
        true  -> format_line(output);
        false -> format_inspect(output)
    end
  end

  defp format_line(line) do
    values = Enum.map(line,
                      fn({_k, v}) ->
                        format_info_item(v)
                      end)

    Enum.join(values, "\t")
  end

  defp format_inspect(output) do
    res = case is_binary(output) do
      true  -> output;
      false -> inspect(output)
    end
  end

  defp format_info_item({k, v}) do
    "{#{k}, #{format_info_item(v)}}"
  end
  defp format_info_item(map) when is_map(map) do
    [ "\#\{",
      Enum.map(map,
               fn({k, v}) ->
                 "#{k} => #{format_info_item(v)}"
               end)
      |> Enum.join(", "),
      "}" ]
    |> to_string
  end
  defp format_info_item(list) when is_list(list) do
    try do
      to_string(list)
    rescue ArgumentError ->
      [ "[",
        Enum.map(list,
                 fn(el) -> 
                   format_info_item(el)
                 end)
        |> Enum.join(", "),
        "]" ]
      |> to_string
    end
  end
  defp format_info_item(val) do
    case String.Chars.impl_for(val) do
      nil -> inspect(val)
      _   -> to_string(val)
    end
  end

  defp erlang_format(fmt, vals) do
    to_string(:io_lib.format(fmt, vals))
  end

  def format_stream(stream, options) do
    Stream.scan(stream, :empty,
                fn
                ({:error, msg}, _) ->
                  {:error, msg};
                (element, _) ->
                  format_element(element, options)
                end)
  end

  defp format_element(val, options) do
    format_output(val, options)
  end
end