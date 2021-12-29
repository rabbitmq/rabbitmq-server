## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.

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
