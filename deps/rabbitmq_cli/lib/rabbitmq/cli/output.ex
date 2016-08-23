## The contents of this file are subject to the Mozilla Public License
## Version 1.1 (the "License"); you may not use this file except in
## compliance with the License. You may obtain a copy of the License
## at http://www.mozilla.org/MPL/
##
## Software distributed under the License is distributed on an "AS IS"
## basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
## the License for the specific language governing rights and
## limitations under the License.
##
## The Original Code is RabbitMQ.
##
## The Initial Developer of the Original Code is GoPivotal, Inc.
## Copyright (c) 2007-2016 Pivotal Software, Inc.  All rights reserved.


defmodule RabbitMQ.CLI.Output do

  alias RabbitMQ.CLI.ExitCodes, as: ExitCodes

  def format_output(output, options) do
    formatter = get_formatter(options)
    result = format_output_0(output, formatter, options)
    result
  end

  def format_output_0({:error, exit_code, error_string}, formatter, options) do
    {:error, exit_code, formatter.format_error(error_string, options)}
  end
  def format_output_0(:ok, _, _) do
    :ok
  end
  def format_output_0({:ok, output}, formatter, options) do
    {:ok, formatter.format_output(output, options)}
  end
  def format_output_0({:stream, stream}, formatter, options) do
    {:stream, formatter.format_stream(stream, options)}
  end

  def print_output(output, options) do
    printer = get_printer(options)
    {:ok, printer_state} = printer.init(options)
    result = print_output_0(output, printer, printer_state)
    printer.finish(printer_state)
    result
  end

  def print_output_0({:error, exit_code, string}, printer, printer_state) do
    printer.print_error(string, printer_state)
    exit_code
  end
  def print_output_0(:ok, printer, printer_state) do
    printer.print_ok(printer_state)
    ExitCodes.exit_ok
  end
  def print_output_0({:ok, single_value}, printer, printer_state) do
    printer.print_output(single_value, printer_state)
    ExitCodes.exit_ok
  end
  def print_output_0({:stream, stream}, printer, printer_state) do
    case print_output_stream(stream, printer, printer_state) do
      :ok               -> ExitCodes.exit_ok;
      {:error, _} = err -> ExitCodes.exit_code_for(err)
    end
  end

  def print_output_stream(stream, printer, printer_state) do
    Enum.reduce_while(stream, :ok,
      fn
      ({:error, err}, _) ->
        printer.print_error(err, printer_state)
        {:halt, {:error, err}};
      (val, _) ->
        printer.print_output(val, printer_state)
        {:cont, :ok}
      end)
  end

  def get_printer(%{printer: printer}) do
    module_name = String.to_atom("RabbitMQ.CLI.Printers." <>
                                 Mix.Utils.camelize(printer))
    case Code.ensure_loaded(module_name) do
      {:module, _}      -> module_name;
      {:error, :nofile} -> default_printer
    end
  end
  def get_printer(_) do
    default_printer
  end

  def get_formatter(%{formatter: formatter}) do
    module_name = Module.safe_concat("RabbitMQ.CLI.Formatters", Mix.Utils.camelize(formatter))
    case Code.ensure_loaded(module_name) do
      {:module, _}      -> module_name;
      {:error, :nofile} -> default_formatter
    end
  end
  def get_formatter(_) do
    default_formatter
  end

  def default_printer() do
    RabbitMQ.CLI.Printers.StdIO
  end

  def default_formatter() do
    RabbitMQ.CLI.Formatters.Inspect
  end
end