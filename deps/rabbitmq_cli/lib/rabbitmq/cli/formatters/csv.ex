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
