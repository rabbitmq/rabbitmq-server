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

defmodule RabbitMQ.CLI.Core.Output do
  def format_output(:ok, _, _) do
    :ok
  end

  # the command intends to produce no output
  def format_output({:ok, nil}, _, _) do
    :ok
  end

  def format_output({:ok, :check_passed}, _, _) do
    :ok
  end

  def format_output({:ok, output}, formatter, options) do
    {:ok, formatter.format_output(output, options)}
  end

  def format_output({:stream, stream}, formatter, options) do
    {:stream, formatter.format_stream(stream, options)}
  end

  def print_output(output, printer, options) do
    {:ok, printer_state} = printer.init(options)
    exit_code = print_output_0(output, printer, printer_state)
    printer.finish(printer_state)
    exit_code
  end

  def print_output_0(:ok, printer, printer_state) do
    printer.print_ok(printer_state)
    :ok
  end

  # the command intends to produce no output
  def print_output_0({:ok, nil}, _printer, _printer_state) do
    :ok
  end

  def print_output_0({:ok, :check_passed}, _printer, _printer_state) do
    :ok
  end

  def print_output_0({:ok, single_value}, printer, printer_state) do
    printer.print_output(single_value, printer_state)
    :ok
  end

  def print_output_0({:stream, stream}, printer, printer_state) do
    case print_output_stream(stream, printer, printer_state) do
      :ok -> :ok
      {:error, _} = err -> err
    end
  end

  def print_output_stream(stream, printer, printer_state) do
    Enum.reduce_while(stream, :ok, fn
      {:error, err}, _ ->
        {:halt, {:error, err}}

      val, _ ->
        printer.print_output(val, printer_state)
        {:cont, :ok}
    end)
  end
end
