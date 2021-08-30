## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2016-2021 VMware, Inc. or its affiliates.  All rights reserved.

defmodule RabbitMQ.CLI.Ctl.Commands.AutocompleteCommand do
  @behaviour RabbitMQ.CLI.CommandBehaviour

  alias RabbitMQ.CLI.Core.{Config, DocGuide}

  def scopes(), do: [:ctl, :diagnostics, :plugins, :queues]

  def distribution(_), do: :none

  def merge_defaults(args, opts) do
    # enforce --silent as shell completion does not
    # expect to receive any additional output, so the command
    # is not really interactive
    {args, Map.merge(opts, %{silent: true})}
  end

  use RabbitMQ.CLI.Core.AcceptsDefaultSwitchesAndTimeout
  use RabbitMQ.CLI.Core.AcceptsOnePositionalArgument

  def run(args, %{script_name: script_name}) do
    {:stream, RabbitMQ.CLI.AutoComplete.complete(script_name, args)}
  end
  def run(args, opts) do
    script_name = Config.get_system_option(:script_name, opts)

    {:stream, RabbitMQ.CLI.AutoComplete.complete(script_name, args)}
  end
  use RabbitMQ.CLI.DefaultOutput

  def usage() do
    "autocomplete [prefix]"
  end

  def banner(_args, _opts) do
    nil
  end

  def usage_doc_guides() do
    [
      DocGuide.cli()
    ]
  end

  def help_section(), do: :help

  def description(), do: "Provides command name autocomplete variants"
end
