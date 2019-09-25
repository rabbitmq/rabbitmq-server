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

defmodule RabbitMQ.CLI.Formatters.JsonStream do
  @moduledoc """
  Formats a potentially infinite stream of maps, proplists, keyword lists,
  and other things that are essentially a map.

  The output exclude JSON array boundaries. The output can be fed
  to `jq' for pretty printing, filtering and querying.
  """

  @behaviour RabbitMQ.CLI.FormatterBehaviour

  alias RabbitMQ.CLI.Formatters.FormatterHelpers
  alias RabbitMQ.CLI.Core.Platform

  def format_output("", _opts) do
    # the empty string can be emitted along with a finishing marker that ends the stream
    # (e.g. with commands that have a duration argument)
    # we just emit the empty string as the last value for the stream in this case
    ""
  end
  def format_output(output, _opts) do
    {:ok, json} = JSON.encode(keys_to_atoms(output))
    json
  end

  def format_stream(stream, options) do
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
        FormatterHelpers.without_errors_2(fn element, _previous ->
          format_element(element, options)
        end)
      )

    elements
  end

  def keys_to_atoms(enum) do
    Enum.map(enum,
             fn({k, v}) when is_binary(k) or is_list(k) ->
                 {String.to_atom(k), v}
               (other) -> other
             end)
  end

  def format_element(val, options) do
    format_output(val, options) <> Platform.line_separator()
  end

  def machine_readable?, do: true
end
