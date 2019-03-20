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

defmodule RabbitMQ.CLI.PrinterBehaviour do
  @callback init(options :: map()) :: {:ok, printer_state :: any} | {:error, error :: any}
  @callback finish(printer_state :: any) :: :ok

  @callback print_output(output :: String.t() | [String.t()], printer_state :: any) :: :ok
  @callback print_ok(printer_state :: any) :: :ok

  def module_name(nil) do
    nil
  end
  def module_name(printer) do
    mod = printer |> String.downcase |> Macro.camelize
    String.to_atom("RabbitMQ.CLI.Printers." <> mod)
  end
end
