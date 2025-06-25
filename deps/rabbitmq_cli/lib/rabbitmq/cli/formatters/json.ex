## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries.  All rights reserved.

# Basic JSON formatter. Supports 1-level of
# collection using start/finish_collection.
# Primary purpose is to translate stream from CTL,
# so there is no need for multiple collection levels
alias RabbitMQ.CLI.Formatters.FormatterHelpers

defmodule RabbitMQ.CLI.Formatters.Json do
  @behaviour RabbitMQ.CLI.FormatterBehaviour

  def format_output(output, opts) when is_bitstring(output) do
    format_output(%{"message" => output}, opts)
  end

  def format_output(output, _opts) do
    {:ok, json} = JSON.encode(keys_to_atoms(convert_erlang_strings(output)))
    json
  end

  def format_stream(stream, options) do
    ## Flatten list_consumers
    elements =
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
      |> Stream.scan(
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

  def keys_to_atoms(enum) do
    Enum.map(
      enum,
      fn
        {k, v} when is_binary(k) or is_list(k) ->
          {String.to_atom(k), v}

        other ->
          other
      end
    )
  end

  def format_element(val, separator, options) do
    separator <> format_output(val, options)
  end

  def machine_readable?, do: true

  # Convert Erlang strings (lists of integers) to binaries for proper JSON encoding
  # Also convert other Erlang-specific terms to readable strings
  defp convert_erlang_strings(data) when is_function(data) do
    "Fun()"
  end

  defp convert_erlang_strings(data) when is_pid(data) do
    "Pid(#{inspect(data)})"
  end

  defp convert_erlang_strings(data) when is_port(data) do
    "Port(#{inspect(data)})"
  end

  defp convert_erlang_strings(data) when is_reference(data) do
    "Ref(#{inspect(data)})"
  end

  defp convert_erlang_strings([]),  do: []

  defp convert_erlang_strings(data) when is_list(data) do
    try do
      case :unicode.characters_to_binary(data, :utf8) do
        binary when is_binary(binary) ->
          # Successfully converted - it was a valid Unicode string
          binary
        _ ->
          # Conversion failed - not a Unicode string, process as regular list
          Enum.map(data, &convert_erlang_strings/1)
      end
    rescue
      ArgumentError ->
        # badarg exception - not valid character data, process as regular list
        Enum.map(data, &convert_erlang_strings/1)
    end
  end

  defp convert_erlang_strings(data) when is_tuple(data) do
    data
    |> Tuple.to_list()
    |> Enum.map(&convert_erlang_strings/1)
    |> List.to_tuple()
  end

  defp convert_erlang_strings(data) when is_map(data) do
    Enum.into(data, %{}, fn {k, v} ->
      {convert_erlang_strings(k), convert_erlang_strings(v)}
    end)
  end

  defp convert_erlang_strings(data), do: data
end
