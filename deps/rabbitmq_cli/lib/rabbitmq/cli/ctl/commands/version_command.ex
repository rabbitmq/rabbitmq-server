## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.

defmodule RabbitMQ.CLI.Ctl.Commands.VersionCommand do
  alias RabbitMQ.CLI.Core.{Validators, Version}

  @behaviour RabbitMQ.CLI.CommandBehaviour

  def scopes(), do: [:ctl, :diagnostics, :plugins]

  use RabbitMQ.CLI.Core.MergesNoDefaults
  use RabbitMQ.CLI.Core.AcceptsNoPositionalArguments

  def validate_execution_environment([] = args, opts) do
    Validators.rabbit_is_loaded(args, opts)
  end

  def run([], %{formatter: "json"}) do
    {:ok, %{version: Version.local_version()}}
  end
  def run([], %{formatter: "csv"}) do
    row = [version: Version.local_version()]
    {:ok, [row]}
  end
  def run([], _opts) do
    {:ok, Version.local_version()}
  end
  use RabbitMQ.CLI.DefaultOutput

  def help_section, do: :help

  def description, do: "Displays CLI tools version"

  def usage, do: "version"

  def banner(_, _), do: nil
end
