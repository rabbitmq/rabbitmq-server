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

## ------------------------ --help option -------------------------------------

  test "--help option prints help for command and exits normally" do
    command = ["status", "--help"]
    assert capture_io(fn ->
      error_check(command, exit_ok())
    end) =~ ~r/Usage/
  end

## ------------------------ Error Messages ------------------------------------
  test "print error message on a bad connection" do
    command = ["status", "-n", "sandwich@pastrami"]
    assert capture_io(:stderr, fn ->
      error_check(command, exit_unavailable())
    end) =~ ~r/unable to perform an operation on node 'sandwich@pastrami'/
  end

  test "print timeout message when an RPC call times out" do
    command = ["list_users", "-t", "0"]
    assert capture_io(:stderr, fn ->
      error_check(command, exit_tempfail())
    end) =~ ~r/Error: operation list_users on node #{get_rabbit_hostname()} timed out. Timeout value used: 0/
  end

  test "print an authentication error message when auth is refused" do
    add_user "kirk", "khaaaaaan"
    command = ["authenticate_user", "kirk", "makeitso"]
    assert capture_io(:stderr,
      fn -> error_check(command, exit_dataerr())
    end) =~ ~r/Error: failed to authenticate user \"kirk\"/
    delete_user "kirk"
  end

## ------------------------ Help and Malformed Commands --------------------------------

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

  test "Empty command with options shows usage, and exit with usage exit code" do
    command = ["-n", "sandwich@pastrami"]
    assert capture_io(:stderr, fn ->
      error_check(command, exit_usage())
    end) =~ ~r/usage/i
  end

  test "Short names without host connect properly" do
    command = ["status", "-n", "rabbit"]
    capture_io(:stderr, fn -> error_check(command, exit_ok()) end)
  end

  test "Unimplemented command shows usage message and returns error" do
    command = ["not_real"]
    assert capture_io(:stderr, fn ->
      error_check(command, exit_usage())
    end) =~ ~r/Usage/
  end

  test "Extraneous arguments return a usage error" do
    command = ["status", "extra"]
    output = capture_io(:stderr, fn ->
      error_check(command, exit_usage())
    end)
    assert output =~ ~r/too many arguments/
    assert output =~ ~r/Usage/
    assert output =~ ~r/status/
  end

  test "Insufficient arguments return a usage error" do
    command = ["list_user_permissions"]
    output = capture_io(:stderr, fn ->
      error_check(command, exit_usage())
    end)
    assert output =~ ~r/not enough arguments/
    assert output =~ ~r/Usage/
    assert output =~ ~r/list_user_permissions/
  end

  test "A bad argument returns a data error" do
    command = ["set_disk_free_limit", "2097152bytes"]
    capture_io(:stderr, fn -> error_check(command, exit_dataerr()) end)
  end

  test "An errored command returns an error code" do
    command = ["delete_user", "voldemort"]
    capture_io(:stderr, fn -> error_check(command, exit_unavailable()) end)
  end

  test "A malformed command with an option as the first command-line arg fails gracefully" do
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

## ------------------------- Auto-complete ------------------------------------

  test "rabbitmqctl auto-completes commands" do
    check_output(["--auto-complete", "rabbitmqctl", "list_q"], "list_queues\n")
    check_output(["--auto-complete", "/usr/bin/rabbitmqctl", "list_q"], "list_queues\n")
    check_output(["--auto-complete", "/my/custom/path/rabbitmqctl", "list_q"], "list_queues\n")
    check_output(["--auto-complete", "rabbitmq-plugins", "enab"], "enable\n")
    check_output(["--auto-complete", "/path/to/rabbitmq-plugins", "enab"], "enable\n")
  end

  test "invalid script name does not autocomplete" do
    check_output(["--auto-complete", "rabbitmqinvalid list"], "")
    check_output(["--auto-complete", "rabbitmqinvalid --script-name rabbitmqctl list"], "")
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
