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

alias RabbitMQ.CLI.Formatters.FormatterHelpers, as: FormatterHelpers

defmodule RabbitMQ.CLI.Formatters.Table do
  import RabbitCommon.Records

  @behaviour RabbitMQ.CLI.FormatterBehaviour

  def format_stream(stream, options) do
    Stream.flat_map(stream,
                fn
                ({:error, msg}) ->
                  {:error, msg};
                (element) ->
                  case format_output(element, options) do
                    list when is_list(list) -> list;
                    other                   -> [other]
                  end
                end)
  end

  def format_output(output, options) when is_map(output) do
    escaped = escaped?(options)
    format_line(output, escaped)
  end
  def format_output([], _) do
    ""
  end
  def format_output([first|_] = output, options)do
    escaped = escaped?(options)
    case Keyword.keyword?(output) do
        true  -> format_line(output, escaped);
        false ->
          case Keyword.keyword?(first) do
            true ->
              Enum.map(output, fn(el) -> format_line(el, escaped) end);
            false ->
              format_inspect(output)
          end
    end
  end
  def format_output(other, _) do
    format_inspect(other)
  end


  defp escaped?(_), do: true

  defp format_line(line, escaped) do
    values = Enum.map(line,
                      fn({_k, v}) ->
                        format_info_item(v, escaped)
                      end)
    Enum.join(values, "\t")
  end

  defp format_inspect(output) do
    case is_binary(output) do
      true  -> output;
      false -> inspect(output)
    end
  end

  defmacro is_u8(x) do
    quote do
      (unquote(x) >= 0 and unquote(x) <= 255)
    end
  end

  defmacro is_u16(x) do
    quote do
      (unquote(x) >= 0 and unquote(x) <= 65_535)
    end
  end

  defp format_info_item(map, escaped) when is_map(map) do
    ["\#\{",
     Enum.map(map,
              fn({k, v}) ->
                ["#{FormatterHelpers.escape(k, escaped)} => ", format_info_item(v, escaped)]
              end)
     |> Enum.join(", "),
     "}"]
  end
  defp format_info_item(resource(name: name), escaped) do # when Record.is_record(res, :resource) do
    #resource(name: name) = res
    FormatterHelpers.escape(name, escaped)
  end
  defp format_info_item({n1, n2, n3, n4} = value, _escaped) when
      is_u8(n1) and is_u8(n2) and is_u8(n3) and is_u8(n4) do
    :rabbit_misc.ntoa(value)
  end
  defp format_info_item({k1, k2, k3, k4, k5, k6, k7, k8} = value, _escaped) when
      is_u16(k1) and is_u16(k2) and is_u16(k3) and is_u16(k4) and
      is_u16(k5) and is_u16(k6) and is_u16(k7) and is_u16(k8) do
    :rabbit_misc.ntoa(value)
  end
  defp format_info_item(value, _escaped) when is_pid(value) do
    :rabbit_misc.pid_to_string(value)
  end
  defp format_info_item(value, escaped) when is_binary(value) do
    FormatterHelpers.escape(value, escaped)
  end
  defp format_info_item(value, escaped) when is_atom(value) do
    FormatterHelpers.escape(to_charlist(value), escaped)
  end
  defp format_info_item([{key, type, _table_entry_value} | _] =
                          value, escaped) when is_binary(key) and
                                               is_atom(type) do
    :io_lib.format("~1000000000000tp",
                   [FormatterHelpers.prettify_amqp_table(value, escaped)])
  end
  defp format_info_item([t | _] = value, escaped)
  when is_tuple(t) or is_pid(t) or is_binary(t) or is_atom(t) or is_list(t) do
    ["[",
       Enum.map(value,
                fn(el) ->
                  format_info_item(el, escaped)
                end)
       |> Enum.join(", "),
       "]"]
  end
  defp format_info_item({key, value}, escaped) do
    ["{" , :io_lib.format("~p", [key]), ", ",
     format_info_item(value, escaped), "}"]
  end
  defp format_info_item(value, _escaped) do
    :io_lib.format("~1000000000000tp", [value])
  end
end
