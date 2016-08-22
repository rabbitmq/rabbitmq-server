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

  def format_output({:error, exit_code, error_string}, options) do
    formatter = get_formatter(options)
    {:error, exit_code, formatter.format_error(error_string, options)}
  end
  def format_output(:ok, _) do
    :ok
  end
  def format_output({:ok, output}, options) do
    formatter = get_formatter(options)
    {:ok, formatter.format_output(output, options)}
  end
  def format_output({:stream, stream}, options) do
    formatter = get_formatter(options)
    {:stream, Stream.map(stream,
                         fn({:error, err}) ->
                           {:error, formatter.format_error(err, options)};
                         (output) ->
                           formatter.format_output(output, options)
                         end)}
  end

  def print_output({:error, exit_code, string}, options) do
    printer = get_printer(options)
    printer.print_error(string, options)
    exit_code
  end
  def print_output(:ok, options) do
    printer = get_printer(options)
    printer.print_ok(options)
    ExitCodes.exit_ok
  end
  def print_output({:ok, single_value}, options) do
    printer = get_printer(options)
    printer.print_output(single_value, options)
    ExitCodes.exit_ok
  end
  def print_output({:stream, stream}, options) do
    printer = get_printer(options)
    printer.start_collection(options)
    exit_code = case print_output_stream(stream, printer, options) do
      :ok               -> ExitCodes.exit_ok;
      {:error, _} = err -> ExitCodes.exit_code_for(err)
    end
    printer.finish_collection(options)
    exit_code
  end

  def print_output_stream(stream, printer, options) do
    Enum.reduce_while(stream, :ok,
      fn
      ({:error, err}, _) ->
        printer.print_error(err, options)
        {:halt, {:error, err}};
      (val, _) ->
        printer.print_output(val, options)
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
    module_name = String.to_atom("RabbitMQ.CLI.Formatters." <>
                                 Mix.Utils.camelize(formatter))
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