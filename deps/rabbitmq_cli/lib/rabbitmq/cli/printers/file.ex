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
