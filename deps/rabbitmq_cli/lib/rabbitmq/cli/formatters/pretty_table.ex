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

alias RabbitMQ.CLI.Formatters.FormatterHelpers

defmodule RabbitMQ.CLI.Formatters.PrettyTable do
  @behaviour RabbitMQ.CLI.FormatterBehaviour

  def format_stream(stream, options) do
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

    # Use rabbit_pretty_stdout module from rabbitmq-common to format the
    # table.
    case entries_with_keys do
      [first_entry | _] ->
        col_headers = [Stream.map(first_entry,
          fn({key, _}) ->
            {to_string(key), :bright_white}
          end)
          |> Enum.to_list()]
        rows = Stream.map(entries_with_keys,
          fn(element) ->
            ret = Stream.map(element,
              fn({_, value}) ->
                # FIXME: Handle all value types.
                {format_value(value), :default}
              end)
              |> Enum.to_list()
            [ret]
          end)
          |> Enum.to_list()
        lines = :rabbit_pretty_stdout.format_table(
          [col_headers | rows],
          :true,
          :true)
        Stream.map(lines,
          fn(line) -> to_string line end)
      [] ->
        entries_with_keys
    end
  end

  def format_output(output, options) do
    string = to_string(:rabbit_misc.format("~p", [output]))
    lines = Stream.map(String.split(string, "\n"),
      fn(element) ->
        [{element, :default}]
      end)
      |> Enum.to_list()
    :rabbit_pretty_stdout.format_table(
      [
        [[{"Output", :bright_white}]],
        lines
      ],
      :true,
      :true)
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
