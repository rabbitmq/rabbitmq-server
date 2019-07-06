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
