## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries.  All rights reserved.

## Prints values from a command as strings(if possible)
defmodule RabbitMQ.CLI.Formatters.EncryptedConfValue do
  alias RabbitMQ.CLI.Core.Helpers
  alias RabbitMQ.CLI.Formatters.FormatterHelpers

  @behaviour RabbitMQ.CLI.FormatterBehaviour

  def format_output(output, _) do
    Helpers.string_or_inspect("encrypted:#{output}")
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
