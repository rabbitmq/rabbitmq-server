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

## Prints values from a command as strings(if possible)
defmodule RabbitMQ.CLI.Formatters.String do
  alias RabbitMQ.CLI.Core.Helpers
  alias RabbitMQ.CLI.Formatters.FormatterHelpers

  @behaviour RabbitMQ.CLI.FormatterBehaviour

  def format_output(output, _) do
    Helpers.string_or_inspect(output)
  end

  def format_stream(stream, options) do
    Stream.map(
      stream,
      FormatterHelpers.without_errors_1(fn el ->
        format_output(el, options)
      end)
    )
  end
end
