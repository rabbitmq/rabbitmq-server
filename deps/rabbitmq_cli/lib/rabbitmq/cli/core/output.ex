## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.

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
