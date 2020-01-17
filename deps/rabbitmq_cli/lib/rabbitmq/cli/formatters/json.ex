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
    {:ok, json} = JSON.encode(keys_to_atoms(output))
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
              true  -> element
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
    Enum.map(enum,
             fn({k, v}) when is_binary(k) or is_list(k) ->
                 {String.to_atom(k), v}
               (other) -> other
             end)
  end

  def format_element(val, separator, options) do
    separator <> format_output(val, options)
  end

  def machine_readable?, do: true
end
