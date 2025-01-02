## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries.  All rights reserved.

defmodule RabbitMQ.CLI.Printers.StdIORaw do
  @behaviour RabbitMQ.CLI.PrinterBehaviour

  def init(_), do: {:ok, :ok}
  def finish(_), do: :ok

  def print_output(nil, _), do: :ok

  def print_output(output, _) when is_list(output) do
    for line <- output do
      IO.write(line)
    end
  end

  def print_output(output, _) do
    IO.write(output)
  end

  def print_ok(_) do
    :ok
  end
end
