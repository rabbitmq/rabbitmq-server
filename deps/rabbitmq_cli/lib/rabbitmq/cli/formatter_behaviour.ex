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
