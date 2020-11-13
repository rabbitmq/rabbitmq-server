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
## Copyright (c) 2007-2018 Pivotal Software, Inc.  All rights reserved.

defmodule RabbitMQ.CLI.Formatters.PrettyTable do
  @behaviour RabbitMQ.CLI.FormatterBehaviour

  require Record
  import Record

  defrecord :table , extract(:table,
    from_lib: "stdout_formatter/include/stdout_formatter.hrl")
  defrecord :cell, extract(:cell,
    from_lib: "stdout_formatter/include/stdout_formatter.hrl")
  defrecord :paragraph, extract(:paragraph,
    from_lib: "stdout_formatter/include/stdout_formatter.hrl")

  def format_stream(stream, _opts) do
    # Flatten for list_consumers
    entries_with_keys = Stream.flat_map(stream,
      fn([first | _] = element) ->
        case Keyword.keyword?(first) or is_map(first) do
          true  -> element;
          false -> [element]
        end
        (other) ->
          [other]
      end)
      |> Enum.to_list()

    # Use stdout_formatter library to format the table.
    case entries_with_keys do
      [first_entry | _] ->
        col_headers = Stream.map(first_entry,
          fn({key, _}) ->
            cell(content: key, props: %{:title => true})
          end)
          |> Enum.to_list()
        rows = Stream.map(entries_with_keys,
          fn(element) ->
            Stream.map(element,
              fn({_, value}) ->
                cell(content: value, props: %{})
              end)
              |> Enum.to_list()
          end)
          |> Enum.to_list()
        ret = :stdout_formatter.to_string(
          table(
            rows: [col_headers | rows],
            props: %{:cell_padding => {0, 1}}))
        [to_string ret]
      [] ->
        entries_with_keys
    end
  end

  def format_output(output, _opts) do
    format = case is_binary(output) do
      true  -> "~s"
      false -> "~p"
    end
    ret = :stdout_formatter.to_string(
      table(
        rows: [
          [cell(content: "Output", props: %{:title => true})],
          [cell(
            content: paragraph(content: output,
            props: %{:format => format}))]],
        props: %{:cell_padding => {0, 1}}))
    to_string ret
  end

  def format_value(value) do
    case is_binary(value) do
      true  -> value
      false -> case is_atom(value) do
        true  -> to_string(value)
        false -> inspect(value)
      end
    end
  end
end
