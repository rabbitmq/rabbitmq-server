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


defmodule RabbitMQCtl do
  alias RabbitMQ.CLI.Distribution,  as: Distribution

  alias RabbitMQ.CLI.Ctl.Commands.HelpCommand, as: HelpCommand

  import RabbitMQ.CLI.Ctl.Helpers
  import  RabbitMQ.CLI.Ctl.Parser
  import RabbitMQ.CLI.ExitCodes

  def main(["--auto-complete", "./rabbitmqctl " <> str]) do
    auto_complete(str)
  end
  def main(["--auto-complete", "rabbitmqctl " <> str]) do
    auto_complete(str)
  end
  def main(unparsed_command) do
    {parsed_cmd, options, invalid} = parse(unparsed_command)
    {printer, print_options} = get_printer(options)
    case try_run_command(parsed_cmd, options, invalid) do
      {:validation_failure, _} = invalid ->
        error_strings = validation_error(invalid, unparsed_command)
        {:error, exit_code_for(invalid), error_strings}
      cmd_result -> cmd_result
    end
    |> print_output(printer, print_options)
    |> exit_program
  end

  def get_printer(%{printer: printer} = opts) do
    module_name = String.to_atom("RabbitMQ.CLI.Printers." <>
                                 Mix.Utils.camelize(printer))
    printer = case Code.ensure_loaded(module_name) do
      {:module, _}      -> module_name;
      {:error, :nofile} -> default_printer
    end
    {printer, opts}
  end
  def get_printer(opts) do
    {default_printer, opts}
  end

  def default_printer() do
    RabbitMQ.CLI.Printers.InspectPrinter
  end

  def print_output({:error, exit_code, strings}, printer, print_options) do
    printer.print_error(Enum.join(strings, "\n"), print_options)
    exit_code
  end
  def print_output(:ok, printer, print_options) do
    printer.print_ok(print_options)
    exit_ok
  end
  def print_output({:ok, single_value}, printer, print_options) do
    printer.print_output(single_value, print_options)
    exit_ok
  end
  def print_output({:stream, stream}, printer, print_options) do
    printer.start_collection(print_options)
    exit_code = case print_output_stream(stream, printer, print_options) do
      :ok               -> exit_ok;
      {:error, _} = err -> exit_code_for(err)
    end
    printer.finish_collection(print_options)
    exit_code
  end

  def print_output_stream(stream, printer, print_options) do
    Enum.reduce_while(stream, :ok,
      fn
      ({:error, err}, _) ->
        printer.print_error(err, print_options)
        {:halt, {:error, err}};
      (val, _) ->
        printer.print_output(val, print_options)
        {:cont, :ok}
      end)
  end

  def try_run_command(parsed_cmd, options, invalid) do
    case {is_command?(parsed_cmd), invalid} do
      ## No such command
      {false, _}  ->
        usage_strings = HelpCommand.all_usage()
        {:error, exit_usage, usage_strings};
      ## Invalid options
      {_, [_|_]}  ->
        {:validation_failure, {:bad_option, invalid}};
      ## Command valid
      {true, []}  ->
        effective_options = options |> merge_all_defaults |> normalize_node
        Distribution.start(effective_options)

        run_command(effective_options, parsed_cmd)
    end
  end

  def auto_complete(str) do
    AutoComplete.complete(str)
    |> Stream.map(&IO.puts/1) |> Stream.run
    exit_program(exit_ok)
  end

  def merge_all_defaults(%{} = options) do
    options
    |> merge_defaults_node
    |> merge_defaults_timeout
    |> merge_defaults_longnames
  end

  defp merge_defaults_node(%{} = opts), do: Map.merge(%{node: get_rabbit_hostname}, opts)

  defp merge_defaults_timeout(%{} = opts), do: Map.merge(%{timeout: :infinity}, opts)

  defp merge_defaults_longnames(%{} = opts), do: Map.merge(%{longnames: false}, opts)

  defp normalize_node(%{node: node} = opts) do
    Map.merge(opts, %{node: parse_node(node)})
  end

  defp maybe_connect_to_rabbitmq("help", _), do: nil
  defp maybe_connect_to_rabbitmq(_, node) do
    connect_to_rabbitmq(node)
  end

  defp run_command(_, []), do: {:error, exit_ok, HelpCommand.all_usage()}
  defp run_command(options, [command_name | arguments]) do
    with_command(command_name,
        fn(command) ->
            case invalid_flags(command, options) do
              [] ->
                {arguments, options} = command.merge_defaults(arguments, options)
                case command.validate(arguments, options) do
                  :ok ->
                    print_banner(command, arguments, options)
                    maybe_connect_to_rabbitmq(command_name, options[:node])

                    command.run(arguments, options)
                    |> command.output(options)
                  err -> err
                end
              result  -> {:validation_failure, {:bad_option, result}}
            end
        end)
  end

  defp with_command(command_name, fun) do
    command = commands[command_name]
    fun.(command)
  end

  defp print_banner(command, args, opts) do
    case command.banner(args, opts) do
     nil -> nil
     banner -> IO.inspect banner
    end
  end

  defp print_standard_messages({:refused, user, _, _} = result, _) do
    IO.puts "Error: failed to authenticate user \"#{user}\""
    result
  end

  defp print_standard_messages(
    {failed_command,
     {:mnesia_unexpectedly_running, node_name}} = result, _)
  when
    failed_command == :reset_failed or
    failed_command == :join_cluster_failed or
    failed_command == :rename_node_failed or
    failed_command == :change_node_type_failed
  do
    IO.puts "Mnesia is still running on node #{node_name}."
    IO.puts "Please stop RabbitMQ with rabbitmqctl stop_app first."
    result
  end

  defp print_standard_messages({:error, :process_not_running} = result, _) do
    IO.puts "Error: process is not running."
    result
  end

  defp print_standard_messages({:error, {:garbage_in_pid_file, _}} = result, _) do
    IO.puts "Error: garbage in pid file."
    result
  end

  defp print_standard_messages({:error, {:could_not_read_pid, err}} = result, _) do
    IO.puts "Error: could not read pid. Detail: #{err}"
    result
  end

  defp print_standard_messages({:healthcheck_failed, message} = result, _) do
    IO.puts "Error: healthcheck failed. Message: #{message}"
    result
  end

  defp validation_error({:validation_failure, err_detail}, unparsed_command) do
    {[command_name | _], _, _} = parse(unparsed_command)
    err = format_validation_error(err_detail, command_name) # TODO format the error better
    base_error = ["Error: #{err}", "Given:\n\t#{unparsed_command |> Enum.join(" ")}"]

    case is_command?(command_name) do
      true  ->
        command = commands[command_name]
        base_error ++ HelpCommand.print_base_usage(HelpCommand.program_name(), command)
      false ->
        base_error ++ HelpCommand.all_usage()
    end
  end

  defp format_validation_error(:not_enough_args, _), do: "not enough arguments."
  defp format_validation_error({:not_enough_args, detail}, _), do: "not enough arguments. #{detail}"
  defp format_validation_error(:too_many_args, _), do: "too many arguments."
  defp format_validation_error({:too_many_args, detail}, _), do: "too many arguments. #{detail}"
  defp format_validation_error(:bad_argument, _), do: "Bad argument."
  defp format_validation_error({:bad_argument, detail}, _), do: "Bad argument. #{detail}"
  defp format_validation_error({:bad_option, opts}, command_name) do
    header = case is_command?(command_name) do
      true  -> "Invalid options for this command:";
      false -> "Invalid options:"
    end
    Enum.join([header | for {key, val} <- opts do "#{key} : #{val}" end], "\n")
  end
  defp format_validation_error(err), do: inspect err

  # defp handle_exit(true), do: handle_exit(:ok, exit_ok)
  # defp handle_exit(:ok), do: handle_exit(:ok, exit_ok)
  # defp handle_exit({:ok, result}), do: handle_exit({:ok, result}, exit_ok)
  # defp handle_exit(result) when is_list(result), do: handle_exit({:ok, result}, exit_ok)
  # defp handle_exit(:ok, code), do: exit_program(code)
  # defp handle_exit({:ok, result}, code) do
  #   case Enumerable.impl_for(result) do
  #     nil -> IO.inspect result;
  #     _   -> result |> Stream.map(&IO.inspect/1) |> Stream.run
  #   end
  #   exit_program(code)
  # end

  defp invalid_flags(command, opts) do
    Map.take(opts, Map.keys(opts) -- (command.flags ++ global_flags))
    |> Map.to_list
  end

  defp exit_program(code) do
    :net_kernel.stop
    exit({:shutdown, code})
  end
end
