## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.

defmodule RabbitMQCtl do
  alias RabbitMQ.CLI.Core.{
    CommandModules,
    Config,
    Distribution,
    ExitCodes,
    Helpers,
    Output,
    Parser
  }
  alias RabbitMQ.CLI.{CommandBehaviour, FormatterBehaviour}
  alias RabbitMQ.CLI.Ctl.Commands.HelpCommand

  # Enable unit tests for private functions
  @compile if Mix.env() == :test, do: :export_all

  @type options() :: map()
  @type command_result() :: {:error, ExitCodes.exit_code(), term()} | term()

  def main(["--auto-complete" | []]) do
    handle_shutdown(:ok)
  end

  def main(unparsed_command) do
    exec_command(unparsed_command, &process_output/3)
    |> handle_shutdown
  end

  def exec_command([] = unparsed_command, _) do
    {_args, parsed_options, _} = Parser.parse_global(unparsed_command)

    # this invocation is considered to be invalid. curl and grep do the
    # same thing.
    {:error, ExitCodes.exit_usage(), Enum.join(HelpCommand.all_usage(parsed_options), "")};
  end
  def exec_command(["--help"] = unparsed_command, _) do
    {_args, parsed_options, _} = Parser.parse_global(unparsed_command)

    # the user asked for --help and we are displaying it to her,
    # reporting a success
    {:ok, ExitCodes.exit_ok(), Enum.join(HelpCommand.all_usage(parsed_options), "")};
  end
  def exec_command(["--version"] = _unparsed_command, opts) do
    # rewrite `--version` as `version`
    exec_command(["version"], opts)
  end
  def exec_command(["--auto-complete" | args], opts) do
    # rewrite `--auto-complete` as `autocomplete`
    exec_command(["autocomplete" | args], opts)
  end
  def exec_command(unparsed_command, output_fun) do
    {command, command_name, arguments, parsed_options, invalid} = Parser.parse(unparsed_command)

    case {command, invalid} do
      {:no_command, _} ->
        command_not_found_string =
          case command_name do
            "" -> ""
            _ -> "\nCommand '#{command_name}' not found. \n"
          end

        usage_string =
          command_not_found_string <>
            Enum.join(HelpCommand.all_usage(parsed_options), "")

        {:error, ExitCodes.exit_usage(), usage_string}

      {{:suggest, suggested}, _} ->
        suggest_message =
          "\nCommand '#{command_name}' not found. \n" <>
            "Did you mean '#{suggested}'? \n"

        {:error, ExitCodes.exit_usage(), suggest_message}

      {_, [_ | _]} ->
        argument_validation_error_output(
          {:bad_option, invalid},
          command,
          unparsed_command,
          parsed_options
        )

      _ ->
        options = parsed_options |> merge_all_defaults |> normalise_options

        try do
          do_exec_parsed_command(unparsed_command, output_fun, arguments, command, options)
        catch error_type, error ->
          maybe_print_stacktrace(error_type, error, __STACKTRACE__, options)
          format_error(error, options, command)
        end
    end
  end

  def do_exec_parsed_command(unparsed_command, output_fun, arguments, command, options) do
    case options[:help] do
      true ->
        {:ok, ExitCodes.exit_ok(), HelpCommand.command_usage(command, options)};
      _ ->
        {arguments, options} = command.merge_defaults(arguments, options)

        maybe_with_distribution(command, options, fn ->
          # rabbitmq/rabbitmq-cli#278
          case Helpers.normalise_node_option(options) do
            {:error, _} = err ->
              format_error(err, options, command)
            {:ok, options} ->
              # The code below implements a tiny decision tree that has
              # to do with CLI argument and environment state validation.
              case command.validate(arguments, options) do
                :ok ->
                  # then optionally validate execution environment
                  case CommandBehaviour.validate_execution_environment(command, arguments, options) do
                    :ok ->
                      result = proceed_to_execution(command, arguments, options)
                      handle_command_output(result, command, options, output_fun)

                    {:validation_failure, err} ->
                      environment_validation_error_output(err, command, unparsed_command, options)

                    {:error, _} = err ->
                      format_error(err, options, command)
                  end

                {:validation_failure, err} ->
                  argument_validation_error_output(err, command, unparsed_command, options)

                {:error, _} = err ->
                  format_error(err, options, command)
              end
          end
        end)
    end
  end

  defp proceed_to_execution(command, arguments, options) do
    maybe_print_banner(command, arguments, options)
    maybe_run_command(command, arguments, options)
  end

  defp maybe_run_command(_, _, %{dry_run: true}) do
    :ok
  end

  defp maybe_run_command(command, arguments, options) do
    try do
      command.run(arguments, options) |> command.output(options)
    catch error_type, error ->
        maybe_print_stacktrace(error_type, error, __STACKTRACE__, options)
        format_error(error, options, command)
    end
  end

  def maybe_print_stacktrace(error_type, :undef = error, stacktrace, _opts) do
    do_print_stacktrace(error_type, error, stacktrace)
  end
  def maybe_print_stacktrace(error_type, :function_clause = error, stacktrace, _opts) do
    do_print_stacktrace(error_type, error, stacktrace)
  end
  def maybe_print_stacktrace(error_type, :badarg = error, stacktrace, _opts) do
    do_print_stacktrace(error_type, error, stacktrace)
  end
  def maybe_print_stacktrace(error_type, {:case_clause, _val} = error, stacktrace, _opts) do
    do_print_stacktrace(error_type, error, stacktrace)
  end
  def maybe_print_stacktrace(error_type, error, stacktrace, %{print_stacktrace: true}) do
    do_print_stacktrace(error_type, error, stacktrace)
  end
  def maybe_print_stacktrace(_error_type, _error, _stacktrace, %{print_stacktrace: false}) do
    nil
  end
  def maybe_print_stacktrace(_error_type, _error, _stacktrace, _opts) do
    nil
  end

  defp do_print_stacktrace(error_type, error, stacktrace) do
    IO.puts("Stack trace: \n")
    IO.puts(Exception.format(error_type, error, stacktrace))
  end

  def handle_command_output(output, command, options, output_fun) do
    case output do
      {:error, _, _} = err ->
        format_error(err, options, command)

      {:error, _} = err ->
        format_error(err, options, command)

      _ ->
        output_fun.(output, command, options)
    end
  end

  defp process_output(output, command, options) do
    formatter = Config.get_formatter(command, options)
    printer = Config.get_printer(command, options)

    output
    |> Output.format_output(formatter, options)
    |> Output.print_output(printer, options)
    |> case do
      {:error, _} = err -> format_error(err, options, command)
      other -> other
    end
  end

  defp output_device(exit_code) do
    case exit_code == ExitCodes.exit_ok() do
      true -> :stdio
      false -> :stderr
    end
  end

  defp handle_shutdown({:error, exit_code, nil}) do
    exit_program(exit_code)
  end

  defp handle_shutdown({_, exit_code, output}) do
    device = output_device(exit_code)

    for line <- List.flatten([output]) do
      IO.puts(device, Helpers.string_or_inspect(line))
    end

    exit_program(exit_code)
  end

  defp handle_shutdown(_) do
    exit_program(ExitCodes.exit_ok())
  end

  def merge_all_defaults(%{} = options) do
    options
    |> merge_defaults_node
    |> merge_defaults_timeout
    |> merge_defaults_longnames
  end

  defp merge_defaults_node(%{} = opts) do
    longnames_opt = Config.get_option(:longnames, opts)
    try do
      default_rabbit_nodename = Helpers.get_rabbit_hostname(longnames_opt)
      Map.merge(%{node: default_rabbit_nodename}, opts)
    catch _error_type, _err ->
      opts
    end
  end

  defp merge_defaults_timeout(%{} = opts), do: Map.merge(%{timeout: :infinity}, opts)

  defp merge_defaults_longnames(%{} = opts), do: Map.merge(%{longnames: false}, opts)

  defp normalise_options(opts) do
    opts |> normalise_timeout
  end

  defp normalise_timeout(%{timeout: timeout} = opts)
       when is_integer(timeout) do
    Map.put(opts, :timeout, timeout * 1000)
  end

  defp normalise_timeout(opts) do
    opts
  end

  # Suppress banner if --quiet option is provided
  defp maybe_print_banner(_, _, %{quiet: true}) do
    nil
  end

  # Suppress banner if --silent option is provided
  defp maybe_print_banner(_, _, %{silent: true}) do
    nil
  end

  defp maybe_print_banner(command, args, opts) do
    # Suppress banner if a machine-readable formatter is used
    formatter = Map.get(opts, :formatter)
    case FormatterBehaviour.machine_readable?(formatter) do
      true  -> nil
      false ->
        case command.banner(args, opts) do
          nil ->
            nil

          banner ->
            case banner do
              list when is_list(list) ->
                for line <- list, do: IO.puts(line)

              binary when is_binary(binary) ->
                IO.puts(binary)
            end
        end
    end
  end

  def argument_validation_error_output(err_detail, command, unparsed_command, options) do
    err = format_validation_error(err_detail)

    base_error =
      "Error (argument validation): #{err}\nArguments given:\n\t#{
        unparsed_command |> Enum.join(" ")
      }"

    validation_error_output(err_detail, base_error, command, options)
  end

  def environment_validation_error_output(err_detail, command, unparsed_command, options) do
    err = format_validation_error(err_detail)
    base_error = "Error: #{err}\nArguments given:\n\t#{unparsed_command |> Enum.join(" ")}"
    validation_error_output(err_detail, base_error, command, options)
  end

  defp validation_error_output(err_detail, base_error, command, options) do
    usage = HelpCommand.base_usage(command, options)
    message = base_error <> "\n" <> usage
    {:error, ExitCodes.exit_code_for({:validation_failure, err_detail}), message}
  end

  defp format_validation_error(:not_enough_args), do: "not enough arguments."
  defp format_validation_error({:not_enough_args, detail}), do: "not enough arguments: #{detail}"
  defp format_validation_error(:too_many_args), do: "too many arguments."
  defp format_validation_error({:too_many_args, detail}), do: "too many arguments: #{detail}"
  defp format_validation_error(:bad_argument), do: "Bad argument."
  defp format_validation_error({:bad_argument, detail}), do: "Bad argument: #{detail}"

  defp format_validation_error({:unsupported_target, details}) do
    details
  end

  defp format_validation_error({:bad_option, opts}) do
    header = "Invalid options for this command:"

    Enum.join(
      [
        header
        | for {key, val} <- opts do
            "#{key} : #{val}"
          end
      ],
      "\n"
    )
  end

  defp format_validation_error({:bad_info_key, keys}),
    do: "Info key(s) #{Enum.join(keys, ",")} are not supported"

  defp format_validation_error(:rabbit_app_is_stopped),
    do:
      "this command requires the 'rabbit' app to be running on the target node. Start it with 'rabbitmqctl start_app'."

  defp format_validation_error(:rabbit_app_is_running),
    do:
      "this command requires the 'rabbit' app to be stopped on the target node. Stop it with 'rabbitmqctl stop_app'."

  defp format_validation_error(:node_running),
    do: "this command requires the target node to be stopped."

  defp format_validation_error(:node_not_running),
    do: "this command requires the target node to be running."

  defp format_validation_error(:unsupported_formatter),
    do: "the requested formatter is not supported by this command"

  defp format_validation_error(err), do: inspect(err)

  defp exit_program(code) do
    :net_kernel.stop()
    exit({:shutdown, code})
  end

  defp format_error({:error, {:node_name, :hostname_not_allowed}}, _, _) do
    {:error, ExitCodes.exit_dataerr(),
      "Unsupported node name: hostname is invalid (possibly contains unsupported characters).\nIf using FQDN node names, use the -l / --longnames argument."}
  end
  defp format_error({:error, {:node_name, :invalid_node_name_head}}, _, _) do
    {:error, ExitCodes.exit_dataerr(),
      "Unsupported node name: node name head (the part before the @) is invalid. Only alphanumerics, _ and - characters are allowed.\nIf using FQDN node names, use the -l / --longnames argument"}
  end
  defp format_error({:error, {:node_name, err_reason} = result}, opts, module) do
    op = CommandModules.module_to_command(module)
    node = opts[:node] || "(failed to parse or compute default value)"
    {:error, ExitCodes.exit_code_for(result),
      "Error: operation #{op} failed due to invalid node name (node: #{node}, reason: #{err_reason}).\nIf using FQDN node names, use the -l / --longnames argument"}
  end

  defp format_error({:error, {:badrpc_multi, :nodedown, [node | _]} = result}, opts, _) do
    diagnostics = get_node_diagnostics(node)

    {:error, ExitCodes.exit_code_for(result),
     badrpc_error_message_header(node, opts) <> diagnostics}
  end

  defp format_error({:error, {:badrpc_multi, :timeout, [node | _]} = result}, opts, module) do
    op = CommandModules.module_to_command(module)

    {:error, ExitCodes.exit_code_for(result),
     "Error: operation #{op} on node #{node} timed out. Timeout value used: #{opts[:timeout]}"}
  end

  defp format_error({:error, {:badrpc, :nodedown} = result}, opts, _) do
    diagnostics = get_node_diagnostics(opts[:node])

    {:error, ExitCodes.exit_code_for(result),
     badrpc_error_message_header(opts[:node], opts) <> diagnostics}
  end

  defp format_error({:error, {:badrpc, :timeout} = result}, opts, module) do
    op = CommandModules.module_to_command(module)

    {:error, ExitCodes.exit_code_for(result),
     "Error: operation #{op} on node #{opts[:node]} timed out. Timeout value used: #{
       opts[:timeout]
     }"}
  end

  defp format_error({:error, {:badrpc, {:timeout, to}} = result}, opts, module) do
    op = CommandModules.module_to_command(module)

    {:error, ExitCodes.exit_code_for(result),
     "Error: operation #{op} on node #{opts[:node]} timed out. Timeout value used: #{to}"}
  end

  defp format_error({:error, {:badrpc, {:timeout, to, warning}}}, opts, module) do
    op = CommandModules.module_to_command(module)

    {:error, ExitCodes.exit_code_for({:timeout, to}),
     "Error: operation #{op} on node #{opts[:node]} timed out. Timeout value used: #{to}. #{
       warning
     }"}
  end

  defp format_error({:error, {:no_such_vhost, vhost} = result}, _opts, _) do
    {:error, ExitCodes.exit_code_for(result), "Virtual host '#{vhost}' does not exist"}
  end

  defp format_error({:error, {:incompatible_version, local_version, remote_version} = result}, _opts, _) do
    {:error, ExitCodes.exit_code_for(result), "Detected potential version incompatibility. CLI tool version: #{local_version}, server: #{remote_version}"}
  end

  defp format_error({:error, {:timeout, to} = result}, opts, module) do
    op = CommandModules.module_to_command(module)

    {:error, ExitCodes.exit_code_for(result),
     "Error: operation #{op} on node #{opts[:node]} timed out. Timeout value used: #{to}"}
  end

  defp format_error({:error, :timeout = result}, opts, module) do
    op = CommandModules.module_to_command(module)

    {:error, ExitCodes.exit_code_for(result),
     "Error: operation #{op} on node #{opts[:node]} timed out. Timeout value used: #{opts[:timeout]}"}
  end

  defp format_error({:error, :timeout, msg}, opts, module) do
    op = CommandModules.module_to_command(module)

    {:error, ExitCodes.exit_code_for(:timeout),
     "Error: operation #{op} on node #{opts[:node]} timed out: #{msg}. Timeout value used: #{opts[:timeout]}"}
  end

  # Plugins
  defp format_error({:error, {:enabled_plugins_mismatch, cli_path, node_path}}, opts, _module) do
    {:error, ExitCodes.exit_dataerr(),
     "Could not update enabled plugins file at #{cli_path}: target node #{opts[:node]} uses a different path (#{
       node_path
     })"}
  end

  defp format_error({:error, {:cannot_read_enabled_plugins_file, path, :eacces}}, _opts, _module) do
    {:error, ExitCodes.exit_dataerr(),
     "Could not read enabled plugins file at #{path}: the file does not exist or permission was denied (EACCES)"}
  end

  defp format_error({:error, {:cannot_read_enabled_plugins_file, path, :enoent}}, _opts, _module) do
    {:error, ExitCodes.exit_dataerr(),
     "Could not read enabled plugins file at #{path}: the file does not exist (ENOENT)"}
  end

  defp format_error({:error, {:cannot_write_enabled_plugins_file, path, :eacces}}, _opts, _module) do
    {:error, ExitCodes.exit_dataerr(),
     "Could not update enabled plugins file at #{path}: the file does not exist or permission was denied (EACCES)"}
  end

  defp format_error({:error, {:cannot_write_enabled_plugins_file, path, :enoent}}, _opts, _module) do
    {:error, ExitCodes.exit_dataerr(),
     "Could not update enabled plugins file at #{path}: the file does not exist (ENOENT)"}
  end

  # Special case health checks. This makes it easier to change
  # output of all health checks at once.
  defp format_error({:error, :check_failed}, %{formatter: "json"}, _) do
    {:error, ExitCodes.exit_unavailable(), nil}
  end
  defp format_error({:error, :check_failed}, _, _) do
    {:error, ExitCodes.exit_unavailable(), nil}
  end
  defp format_error({:error, :check_failed, err}, %{formatter: "json"}, _) when is_map(err) do
    {:ok, res} = JSON.encode(err)
    {:error, ExitCodes.exit_unavailable(), res}
  end
  defp format_error({:error, :check_failed, err}, %{formatter: "json"}, _) do
    {:error, ExitCodes.exit_unavailable(), err}
  end
  defp format_error({:error, :check_failed, err}, _, _) do
    {:error, ExitCodes.exit_unavailable(), err}
  end

  defp format_error({:error, nil}, _, _) do
    # the command intends to produce no output, e.g. a return code
    # is sufficient
    {:error, ExitCodes.exit_unavailable(), nil}
  end

  # Catch all clauses
  defp format_error({:error, err}, %{formatter: "json"}, _) when is_map(err) do
    {:ok, res} = JSON.encode(err)
    {:error, ExitCodes.exit_unavailable(), res}
  end
  defp format_error({:error, exit_code, err}, %{formatter: "json"}, _) when is_map(err) do
    {:ok, res} = JSON.encode(err)
    {:error, exit_code, res}
  end

  defp format_error({:error, exit_code, err}, _, _) do
    string_err = Helpers.string_or_inspect(err)

    {:error, exit_code, "Error:\n#{string_err}"}
  end

  defp format_error({:error, err} = result, _, _) do
    string_err = Helpers.string_or_inspect(err)

    {:error, ExitCodes.exit_code_for(result), "Error:\n#{string_err}"}
  end

  defp format_error(error, _opts, _module) do
    {:error, ExitCodes.exit_software(), "#{inspect(error)}\n"}
  end

  defp get_node_diagnostics(nil) do
    "Target node is not defined"
  end

  defp get_node_diagnostics(node_name) do
    to_string(:rabbit_nodes_common.diagnostics([node_name]))
  end

  defp badrpc_error_message_header(node, _opts) do
    """
    Error: unable to perform an operation on node '#{node}'. Please see diagnostics information and suggestions below.

    Most common reasons for this are:

     * Target node is unreachable (e.g. due to hostname resolution, TCP connection or firewall issues)
     * CLI tool fails to authenticate with the server (e.g. due to CLI tool's Erlang cookie not matching that of the server)
     * Target node is not running

    In addition to the diagnostics info below:

     * See the CLI, clustering and networking guides on https://rabbitmq.com/documentation.html to learn more
     * Consult server logs on node #{node}
     * If target node is configured to use long node names, don't forget to use --longnames with CLI tools
    """
  end

  ## Tries to enable erlang distribution, which can be configured
  ## via distribution callback in the command as :cli, :none or {:custom, fun()}.
  ## :cli - default rabbitmqctl node name
  ## :none - do not start a distribution (e.g. offline command)
  ## {:fun, fun} - run a custom function to enable distribution.
  ## custom mode is usefult for commands which should have specific node name.
  ## Runs code if distribution is successful, or not needed.
  @spec maybe_with_distribution(module(), options(), (() -> command_result())) :: command_result()
  defp maybe_with_distribution(command, options, code) do
    try do
      maybe_with_distribution_without_catch(command, options, code)
    catch error_type, error ->
      maybe_print_stacktrace(error_type, error, __STACKTRACE__, options)
      format_error(error, options, command)
    end
  end

  defp maybe_with_distribution_without_catch(command, options, code) do
    distribution_type = CommandBehaviour.distribution(command, options)

    case distribution_type do
      :none ->
        code.()

      :cli ->
        case Distribution.start(options) do
          :ok ->
            code.()

          {:ok, _} ->
            code.()

          {:error, reason} ->
            {:error, ExitCodes.exit_config(), "Distribution failed: #{inspect(reason)}"}
        end

      {:fun, fun} ->
        case fun.(options) do
          :ok ->
            code.()

          {:error, reason} ->
            {:error, ExitCodes.exit_config(), "Distribution failed: #{inspect(reason)}"}
        end
    end
  end
end
