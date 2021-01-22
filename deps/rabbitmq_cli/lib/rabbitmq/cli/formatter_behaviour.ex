## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.

# Formats returned values e.g. to human-readable text or JSON.
defmodule RabbitMQ.CLI.FormatterBehaviour do
  alias RabbitMQ.CLI.Core.Helpers

  @callback format_output(any, map()) :: String.t() | [String.t()]
  @callback format_stream(Enumerable.t(), map()) :: Enumerable.t()

  @optional_callbacks switches: 0,
                      aliases: 0

  @callback switches() :: Keyword.t()
  @callback aliases() :: Keyword.t()

  def switches(formatter) do
    Helpers.apply_if_exported(formatter, :switches, [], [])
  end

  def aliases(formatter) do
    Helpers.apply_if_exported(formatter, :aliases, [], [])
  end

  def module_name(nil) do
    nil
  end
  def module_name(formatter) do
    mod = formatter |> String.downcase |> Macro.camelize
    Module.safe_concat("RabbitMQ.CLI.Formatters", mod)
  end

  def machine_readable?(nil) do
    false
  end
  def machine_readable?(formatter) do
    Helpers.apply_if_exported(module_name(formatter), :machine_readable?, [], false)
  end
end
