## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.

alias RabbitMQ.CLI.Formatters.FormatterHelpers

defmodule RabbitMQ.CLI.Formatters.Table do
  @behaviour RabbitMQ.CLI.FormatterBehaviour

  def switches(), do: [table_headers: :boolean, pad_to_header: :boolean]

  def format_stream(stream, options) do
    # Flatten for list_consumers
    Stream.flat_map(
      stream,
      fn
        [first | _] = element ->
          case FormatterHelpers.proplist?(first) or is_map(first) do
            true -> element
            false -> [element]
          end

        other ->
          [other]
      end
    )
    |> Stream.transform(
      :init,
      FormatterHelpers.without_errors_2(fn
        element, :init ->
          {maybe_header(element, options), :next}

        element, :next ->
          {[format_output_1(element, options)], :next}
      end)
    )
  end

  def format_output(output, options) do
    maybe_header(output, options)
  end

  defp maybe_header(output, options) do
    opt_table_headers = Map.get(options, :table_headers, true)
    opt_silent = Map.get(options, :silent, false)

    case {opt_silent, opt_table_headers} do
      {true, _} ->
        [format_output_1(output, options)]

      {false, false} ->
        [format_output_1(output, options)]

      {false, true} ->
        format_header(output) ++ [format_output_1(output, options)]
    end
  end

  defp format_output_1(output, options) when is_map(output) do
    escaped = escaped?(options)
    pad_to_header = pad_to_header?(options)
    format_line(output, escaped, pad_to_header)
  end

  defp format_output_1([], _) do
    ""
  end

  defp format_output_1(output, options) do
    escaped = escaped?(options)
    pad_to_header = pad_to_header?(options)

    case FormatterHelpers.proplist?(output) do
      true -> format_line(output, escaped, pad_to_header)
      false -> format_inspect(output)
    end
  end

  defp escaped?(_), do: true

  defp pad_to_header?(%{pad_to_header: pad}), do: pad
  defp pad_to_header?(_), do: false

  defp format_line(line, escaped, pad_to_header) do
    values =
      Enum.map(
        line,
        fn {k, v} ->
          line = FormatterHelpers.format_info_item(v, escaped)

          case pad_to_header do
            true ->
              String.pad_trailing(
                to_string(line),
                String.length(to_string(k))
              )

            false ->
              line
          end
        end
      )

    Enum.join(values, "\t")
  end

  defp format_inspect(output) do
    case is_binary(output) do
      true -> output
      false -> inspect(output)
    end
  end

  @spec format_header(term()) :: [String.t()]
  defp format_header(output) do
    keys =
      case output do
        map when is_map(map) ->
          Map.keys(map)

        keyword when is_list(keyword) ->
          case FormatterHelpers.proplist?(keyword) do
            true -> Keyword.keys(keyword)
            false -> []
          end

        _ ->
          []
      end

    case keys do
      [] -> []
      _ -> [Enum.join(keys, "\t")]
    end
  end
end
