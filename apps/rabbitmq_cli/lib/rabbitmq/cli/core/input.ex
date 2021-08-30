## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.

defmodule RabbitMQ.CLI.Core.Input do
  alias RabbitMQ.CLI.Core.Config

  def consume_single_line_string_with_prompt(prompt, opts) do
    val = case Config.output_less?(opts) do
      true  ->
        IO.read(:stdio, :line)
      false ->
        IO.puts(prompt)
        IO.read(:stdio, :line)
    end

    case val do
      :eof -> :eof
      ""   -> :eof
      s    -> String.trim(s)
    end
  end

  def consume_multiline_string() do
    val = IO.read(:stdio, :all)

    case val do
      :eof -> :eof
      ""   -> :eof
      s    -> String.trim(s)
    end
  end

  def infer_password(prompt, opts) do
    consume_single_line_string_with_prompt(prompt, opts)
  end
end
