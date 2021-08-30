## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.

defmodule RabbitMQ.CLI.Ctl.Commands.RotateLogsCommand do
  alias RabbitMQ.CLI.Core.{DocGuide, ExitCodes}

  @behaviour RabbitMQ.CLI.CommandBehaviour

  use RabbitMQ.CLI.Core.MergesNoDefaults
  use RabbitMQ.CLI.Core.AcceptsNoPositionalArguments
  use RabbitMQ.CLI.Core.RequiresRabbitAppRunning

  def run([], _) do
    {
      :error,
      ExitCodes.exit_unavailable(),
      "This command does not rotate logs anymore [deprecated]"
    }
  end
  use RabbitMQ.CLI.DefaultOutput

  def usage, do: "rotate_logs"

  def usage_doc_guides() do
    [
      DocGuide.logging()
    ]
  end

  def help_section(), do: :node_management

  def description(), do: "Does nothing [deprecated]"

  def banner(_, _), do: nil
end
