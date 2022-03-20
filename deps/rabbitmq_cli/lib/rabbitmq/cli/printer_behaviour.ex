## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.

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
