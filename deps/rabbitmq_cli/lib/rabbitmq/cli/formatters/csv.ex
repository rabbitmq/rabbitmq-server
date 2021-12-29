## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.

alias RabbitMQ.CLI.Formatters.FormatterHelpers

defmodule RabbitMQ.CLI.Formatters.Csv do
  @behaviour RabbitMQ.CLI.FormatterBehaviour

  def format_stream(stream, _) do
    ## Flatten list_consumers
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
    ## Add info_items names
    |> Stream.transform(
      :init,
      FormatterHelpers.without_errors_2(fn
        element, :init ->
          {
            case keys(element) do
              nil -> [values(element)]
              ks -> [ks, values(element)]
            end,
            :next
          }

        element, :next ->
          {[values(element)], :next}
      end)
    )
    |> CSV.encode(delimiter: "")
  end

  def format_output(output, _) do
    case keys(output) do
      nil -> [values(output)]
      ks -> [ks, values(output)]
    end
    |> CSV.encode()
    |> Enum.join()
  end

  def machine_readable?, do: true

  #
  # Implementation
  #

  defp keys(map) when is_map(map) do
    Map.keys(map)
  end

  defp keys(list) when is_list(list) do
    case FormatterHelpers.proplist?(list) do
      true -> Keyword.keys(list)
      false -> nil
    end
  end

  defp keys(_other) do
    nil
  end

  defp values(map) when is_map(map) do
    Map.values(map)
  end

  defp values([]) do
    []
  end

  defp values(list) when is_list(list) do
    case FormatterHelpers.proplist?(list) do
      true -> Keyword.values(list)
      false -> list
    end
  end

  defp values(other) do
    other
  end
end

defimpl CSV.Encode, for: PID do
  def encode(pid, env \\ []) do
    FormatterHelpers.format_info_item(pid)
    |> to_string
    |> CSV.Encode.encode(env)
  end
end

defimpl CSV.Encode, for: List do
  def encode(list, env \\ []) do
    FormatterHelpers.format_info_item(list)
    |> to_string
    |> CSV.Encode.encode(env)
  end
end

defimpl CSV.Encode, for: Tuple do
  def encode(tuple, env \\ []) do
    FormatterHelpers.format_info_item(tuple)
    |> to_string
    |> CSV.Encode.encode(env)
  end
end

defimpl CSV.Encode, for: Map do
  def encode(map, env \\ []) do
    FormatterHelpers.format_info_item(map)
    |> to_string
    |> CSV.Encode.encode(env)
  end
end
