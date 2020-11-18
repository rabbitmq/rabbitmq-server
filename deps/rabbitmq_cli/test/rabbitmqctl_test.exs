## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2020 VMware, Inc. or its affiliates.  All rights reserved.


defmodule RabbitMQCtlTest do
  use ExUnit.Case, async: false
  import ExUnit.CaptureIO
  import RabbitMQ.CLI.Core.ExitCodes
  import TestHelper


  setup_all do
    RabbitMQ.CLI.Core.Distribution.start()

    set_scope(:all)

    :ok
  end

  #
  # --help and `help [command]`
  #

  test "--help option prints help for the command and exits with an OK" do
    command = ["status", "--help"]
    assert capture_io(fn ->
      error_check(command, exit_ok())
    end) =~ ~r/Usage/
  end

  test "bare --help prints general help and exits with an OK" do
    command = ["--help"]
    assert capture_io(fn ->
      error_check(command, exit_ok())
    end) =~ ~r/Usage/
  end

  test "help [command] prints help for the command and exits with an OK" do
    command = ["help", "status"]
    assert capture_io(fn ->
      error_check(command, exit_ok())
    end) =~ ~r/Usage/
  end

  test "bare help command prints general help and exits with an OK" do
    command = ["help"]
    assert capture_io(fn ->
      error_check(command, exit_ok())
    end) =~ ~r/Usage/
  end

  #
  # Validation and Error Handling
  #

  test "print error message on a bad connection" do
    command = ["status", "-n", "sandwich@pastrami"]
    assert capture_io(:stderr, fn ->
      error_check(command, exit_unavailable())
    end) =~ ~r/unable to perform an operation on node 'sandwich@pastrami'/
  end

  test "when an RPC call times out, prints a timeout message" do
    command = ["list_users", "-t", "0"]
    assert capture_io(:stderr, fn ->
      error_check(command, exit_tempfail())
    end) =~ ~r/Error: operation list_users on node #{get_rabbit_hostname()} timed out. Timeout value used: 0/
  end

  test "when authentication fails, prints an authentication error message" do
    add_user "kirk", "khaaaaaan"
    command = ["authenticate_user", "kirk", "makeitso"]
    assert capture_io(:stderr,
      fn -> error_check(command, exit_dataerr())
    end) =~ ~r/Error: failed to authenticate user \"kirk\"/
    delete_user "kirk"
  end

  test "when invoked without arguments, displays a generic usage message and exits with a non-zero code" do
    command = []
    assert capture_io(:stderr, fn ->
      error_check(command, exit_usage())
    end) =~ ~r/usage/i
  end

  test "when invoked with only a --help, shows a generic usage message and exits with a success" do
    command = ["--help"]
    assert capture_io(:stdio, fn ->
      error_check(command, exit_ok())
    end) =~ ~r/usage/i
  end

  test "when invoked with --help [command], shows a generic usage message and exits with a success" do
    command = ["--help", "status"]
    assert capture_io(:stdio, fn ->
      error_check(command, exit_ok())
    end) =~ ~r/usage/i
  end

  test "when no command name is provided, displays usage" do
    command = ["-n", "sandwich@pastrami"]
    assert capture_io(:stderr, fn ->
      error_check(command, exit_usage())
    end) =~ ~r/usage/i
  end

  test "short node name without the host part connects properly" do
    command = ["status", "-n", "rabbit"]
    capture_io(:stderr, fn -> error_check(command, exit_ok()) end)
  end

  test "a non-existent command results in help message displayed" do
    command = ["not_real"]
    assert capture_io(:stderr, fn ->
      error_check(command, exit_usage())
    end) =~ ~r/Usage/
  end

  test "a command that's been provided extra arguments exits with a reasonable error code" do
    command = ["status", "extra"]
    output = capture_io(:stderr, fn ->
      error_check(command, exit_usage())
    end)
    assert output =~ ~r/too many arguments/
    assert output =~ ~r/Usage/
    assert output =~ ~r/status/
  end

  test "a command that's been provided insufficient arguments exits with a reasonable error code" do
    command = ["list_user_permissions"]
    output = capture_io(:stderr, fn ->
      error_check(command, exit_usage())
    end)
    assert output =~ ~r/not enough arguments/
    assert output =~ ~r/Usage/
    assert output =~ ~r/list_user_permissions/
  end

  test "a command that's provided an invalid argument exits a reasonable error" do
    command = ["set_disk_free_limit", "2097152bytes"]
    capture_io(:stderr, fn -> error_check(command, exit_dataerr()) end)
  end

  test "a command that fails with an error exits with a reasonable error code" do
    command = ["delete_user", "voldemort"]
    capture_io(:stderr, fn -> error_check(command, exit_nouser()) end)
  end

  test "a mcommand with an unsupported option as the first command-line arg fails gracefully" do
    command1 = ["--invalid=true", "list_permissions", "-p", "/"]
    assert capture_io(:stderr, fn ->
      error_check(command1, exit_usage())
    end) =~ ~r/Invalid options for this command/

    command2 = ["--node", "rabbit", "status", "quack"]
    assert capture_io(:stderr, fn ->
      error_check(command2, exit_usage())
    end) =~ ~r/too many arguments./

    command3 = ["--node", "rabbit", "add_user"]
    assert capture_io(:stderr, fn ->
      error_check(command3, exit_usage())
    end) =~ ~r/not enough arguments./
  end

## ------------------------- Default Flags ------------------------------------

  test "an empty node option is filled with the default rabbit node" do
    assert RabbitMQCtl.merge_all_defaults(%{})[:node] ==
      TestHelper.get_rabbit_hostname()
  end

  test "a non-empty node option is not overwritten" do
    assert RabbitMQCtl.merge_all_defaults(%{node: :jake@thedog})[:node] ==
      :jake@thedog
  end

  test "an empty timeout option is set to infinity" do
    assert RabbitMQCtl.merge_all_defaults(%{})[:timeout] == :infinity
  end

  test "a non-empty timeout option is not overridden" do
    assert RabbitMQCtl.merge_all_defaults(%{timeout: 60})[:timeout] == 60
  end

  test "other parameters are not overridden by the default" do
    assert RabbitMQCtl.merge_all_defaults(%{vhost: "quack"})[:vhost] == "quack"
  end

  test "any flags that aren't global or command-specific cause a bad option" do
    command1 = ["status", "--nod=rabbit"]
    assert capture_io(:stderr, fn ->
      error_check(command1, exit_usage())
    end) =~ ~r/Invalid options for this command/

    command2 = ["list_permissions", "-o", "/"]
    assert capture_io(:stderr, fn ->
      error_check(command2, exit_usage())
    end) =~ ~r/Invalid options for this command/
  end

  #
  # --auto-complete and `autocomplete [command]`
  #

  test "--auto-complete delegates to the autocomplete command" do
    # Note: these are not script name (scope) aware without --script-name
    # but the actual command invoked in a shell will be
    check_output(["--auto-complete", "list_q"], "list_queues\n")
    check_output(["--auto-complete", "list_con", "--script-name", "rabbitmq-diagnostics"], "list_connections\nlist_consumers\n")
    check_output(["--auto-complete", "--script-name", "rabbitmq-diagnostics", "mem"], "memory_breakdown\n")
    check_output(["--auto-complete", "--script-name", "rabbitmq-queues", "add_m"], "add_member\n")
  end

  test "autocompletion command used directly" do
    # Note: these are not script name (scope) aware without --script-name
    # but the actual command invoked in a shell will be
    check_output(["autocomplete", "list_q"], "list_queues\n")
    check_output(["autocomplete", "list_con", "--script-name", "rabbitmq-diagnostics"], "list_connections\nlist_consumers\n")
    check_output(["autocomplete", "--script-name", "rabbitmq-diagnostics", "mem"], "memory_breakdown\n")
    check_output(["autocomplete", "--script-name", "rabbitmq-queues", "add_m"], "add_member\n")
  end

  defp check_output(cmd, out) do
    assert capture_io(fn ->
      error_check(cmd, exit_ok())
    end) == out
  end


## ------------------------- Error formatting ---------------------------------

  test "badrpc nodedown error" do
    exit_code = exit_unavailable()
    node = :example@node
    {:error, ^exit_code, message} =
        RabbitMQCtl.handle_command_output(
          {:error, {:badrpc, :nodedown}},
          :no_command, %{node: node},
          fn(output, _, _) -> output end)

    assert message =~ ~r/Error: unable to perform an operation on node/
    assert message =~ ~r/DIAGNOSTICS/
    assert message =~ ~r/attempted to contact/

    localnode = :non_existent_node@localhost
    {:error, ^exit_code, message} =
        RabbitMQCtl.handle_command_output(
          {:error, {:badrpc, :nodedown}},
          :no_command, %{node: localnode},
          fn(output, _, _) -> output end)
    assert message =~ ~r/DIAGNOSTICS/
    assert message =~ ~r/attempted to contact/
    assert message =~ ~r/suggestion: start the node/
  end

  test "badrpc timeout error" do
    exit_code = exit_tempfail()
    timeout = 1000
    nodename = :node@host
    err_msg = "Error: operation example on node node@host timed out. Timeout value used: #{timeout}"
    {:error, ^exit_code, ^err_msg} =
      RabbitMQCtl.handle_command_output(
          {:error, {:badrpc, :timeout}},
          ExampleCommand, %{timeout: timeout, node: nodename},
          fn(output, _, _) -> output end)
  end

  test "generic error" do
    exit_code = exit_unavailable()
    {:error, ^exit_code, "Error:\nerror message"} =
      RabbitMQCtl.handle_command_output(
        {:error, "error message"},
        :no_command, %{},
        fn(output, _, _) -> output end)
  end

  test "inspect arbitrary error" do
    exit_code = exit_unavailable()
    error = %{i: [am: "arbitrary", error: 1]}
    inspected = inspect(error)
    {:error, ^exit_code, "Error:\n" <> ^inspected} =
      RabbitMQCtl.handle_command_output(
        {:error, error},
        :no_command, %{},
        fn(output, _, _) -> output end)
  end

  test "atom error" do
    exit_code = exit_unavailable()
    {:error, ^exit_code, "Error:\nerror_message"} =
      RabbitMQCtl.handle_command_output(
        {:error, :error_message},
        :no_command, %{},
        fn(output, _, _) -> output end)
  end

end
