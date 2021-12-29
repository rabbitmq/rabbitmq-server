## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.

defmodule RabbitMQ.CLI.Printers.File do
  @behaviour RabbitMQ.CLI.PrinterBehaviour

  def init(options) do
    file = options[:file]

    case File.open(file) do
      {:ok, io_device} -> {:ok, %{device: io_device}}
      {:error, err} -> {:error, err}
    end
  end

  def finish(%{device: io_device}) do
    :ok = File.close(io_device)
  end

  def print_output(output, %{device: io_device}) when is_list(output) do
    for line <- output do
      IO.puts(io_device, line)
    end
  end

  def print_output(output, %{device: io_device}) do
    IO.puts(io_device, output)
  end

  def print_ok(_) do
    :ok
  end
end
