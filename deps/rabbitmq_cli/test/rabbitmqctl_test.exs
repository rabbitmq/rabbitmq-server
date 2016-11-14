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


defmodule RabbitMQCtlTest do
  use ExUnit.Case, async: false
  import ExUnit.CaptureIO
  import RabbitMQ.CLI.Core.ExitCodes
  import TestHelper


  setup_all do
    RabbitMQ.CLI.Core.Distribution.start()
    :net_kernel.connect_node(get_rabbit_hostname)

    on_exit([], fn ->
      :erlang.disconnect_node(get_rabbit_hostname)

    end)

    :ok
  end

## ------------------------ Error Messages ------------------------------------
  test "print error message on a bad connection" do
    command = ["status", "-n", "sandwich@pastrami"]
    assert capture_io(:stderr, fn ->
      error_check(command, exit_unavailable)
    end) =~ ~r/unable to connect to node 'sandwich@pastrami'\: nodedown/
  end

  test "print timeout message when an RPC call times out" do
    command = ["list_users", "-t", "0"]
    assert capture_io(:stderr, fn ->
      error_check(command, exit_tempfail)
    end) =~ ~r/Error: operation list_users on node #{get_rabbit_hostname} timed out. Timeout: 0/
  end

  test "print an authentication error message when auth is refused" do
    add_user "kirk", "khaaaaaan"
    command = ["authenticate_user", "kirk", "makeitso"]
    assert capture_io(:stderr,
      fn -> error_check(command, exit_dataerr)
    end) =~ ~r/Error: failed to authenticate user \"kirk\"/
    delete_user "kirk"
  end

## ------------------------ Malformed Commands --------------------------------

  test "Empty command shows usage message" do
    command = []
    assert capture_io(:stderr, fn ->
      error_check(command, exit_usage)
    end) =~ ~r/Usage:\n/
  end

  test "Empty command with options shows usage, and exit with usage exit code" do
    command = ["-n", "sandwich@pastrami"]
    assert capture_io(:stderr, fn ->
      error_check(command, exit_usage)
    end) =~ ~r/Usage:\n/
  end

  test "Short names without host connect properly" do
    command = ["status", "-n", "rabbit"]
    capture_io(:stderr, fn -> error_check(command, exit_ok) end)
  end

  test "Unimplemented command shows usage message and returns error" do
    command = ["not_real"]
    assert capture_io(:stderr, fn ->
      error_check(command, exit_usage)
    end) =~ ~r/Usage\:/
  end

  test "Extraneous arguments return a usage error" do
    command = ["status", "extra"]
    assert capture_io(:stderr, fn ->
      error_check(command, exit_usage)
    end) =~ ~r/Given:\n\t.*\nUsage:\n.* status/
  end

  test "Insufficient arguments return a usage error" do
    command = ["list_user_permissions"]
    assert capture_io(:stderr, fn ->
      error_check(command, exit_usage)
    end) =~ ~r/Given:\n\t.*\nUsage:\n.* list_user_permissions/
  end

  test "A bad argument returns a data error" do
    command = ["set_disk_free_limit", "2097152bytes"]
    capture_io(:stderr, fn -> error_check(command, exit_dataerr) end)
  end

  test "An errored command returns an error code" do
    command = ["delete_user", "voldemort"]
    capture_io(:stderr, fn -> error_check(command, exit_software) end)
  end

  test "A malformed command with an option as the first command-line arg fails gracefully" do
    command1 = ["--invalid=true", "list_permissions", "-p", "/"]
    assert capture_io(:stderr, fn ->
      error_check(command1, exit_usage)
    end) =~ ~r/Error: Invalid options for this command/

    command2 = ["--node", "rabbit", "status", "quack"]
    assert capture_io(:stderr, fn ->
      error_check(command2, exit_usage)
    end) =~ ~r/Error: too many arguments./

    command3 = ["--node", "rabbit", "add_user", "quack"]
    assert capture_io(:stderr, fn ->
      error_check(command3, exit_usage)
    end) =~ ~r/Error: not enough arguments./
  end

## ------------------------- Default Flags ------------------------------------

  test "an empty node option is filled with the default rabbit node" do
    assert RabbitMQCtl.merge_all_defaults(%{})[:node] ==
      TestHelper.get_rabbit_hostname
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
      error_check(command1, exit_usage)
    end) =~ ~r/Error: Invalid options for this command/

    command2 = ["list_permissions", "-o", "/"]
    assert capture_io(:stderr, fn ->
      error_check(command2, exit_usage)
    end) =~ ~r/Error: Invalid options for this command/
  end
end
