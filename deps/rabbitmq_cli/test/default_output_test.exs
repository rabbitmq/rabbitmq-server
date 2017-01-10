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
## Copyright (c) 2007-2017 Pivotal Software, Inc.  All rights reserved.


defmodule DefaultOutputTest do
  use ExUnit.Case, async: false
  import RabbitMQ.CLI.Core.ExitCodes

  test "ok is passed as is" do
    assert match?(:ok, ExampleCommand.output(:ok, %{}))
  end

  test "ok with message is passed as is" do
    assert match?({:ok, :message}, ExampleCommand.output({:ok, :message}, %{}))
    assert match?({:ok, {:complex, "message"}}, ExampleCommand.output({:ok, {:complex, "message"}}, %{}))
  end

  test "enumerable is passed as stream" do
    assert match?({:stream, 'list'}, ExampleCommand.output({:ok, 'list'}, %{}))
    assert match?({:stream, 'list'}, ExampleCommand.output('list', %{}))

    assert match?({:stream, [1,2,3]}, ExampleCommand.output({:ok, [1,2,3]}, %{}))
    assert match?({:stream, [1,2,3]}, ExampleCommand.output([1,2,3], %{}))

    stream = Stream.timer(10000)
    assert match?({:stream, ^stream}, ExampleCommand.output({:ok, stream}, %{}))
    assert match?({:stream, ^stream}, ExampleCommand.output(stream, %{}))
  end

  test "badrpc nodedown error" do
    exit_code = exit_unavailable
    node = :example@node
    assert match?({:error, ^exit_code, "Error: unable to connect to node 'example@node': nodedown"},
                  ExampleCommand.output({:badrpc, :nodedown}, %{node: node}))
  end

  test "badrpc timeout error" do
    exit_code = exit_tempfail
    timeout = 1000
    nodename = :node@host
    assert match?({:error, ^exit_code, "Error: operation example on node node@host timed out. Timeout: 1000"},
                  ExampleCommand.output({:badrpc, :timeout}, %{timeout: timeout, node: nodename}))
  end

  test "generic error" do
    exit_code = exit_software
    assert match?({:error, ^exit_code, "Error:\nerror message"},
                  ExampleCommand.output({:error, "error message"}, %{}))
  end

  test "inspect arbitrary error" do
    exit_code = exit_software
    error = %{i: [am: "arbitrary", error: 1]}
    inspected = inspect(error)
    assert match?({:error, ^exit_code, "Error:\n" <> ^inspected},
                  ExampleCommand.output({:error, error}, %{}))
  end

  test "atom error" do
    exit_code = exit_software
    assert match?({:error, ^exit_code, "Error:\nerror_message"},
                  ExampleCommand.output({:error, :error_message}, %{}))
  end

  test "unknown atom is error" do
    exit_code = exit_software
    assert match?({:error, ^exit_code, "Error:\nerror_message"},
                  ExampleCommand.output(:error_message, %{}))
  end

  test "unknown tuple is error" do
    exit_code = exit_software
    assert match?({:error, ^exit_code, "Error:\n{:left, :right}"},
                  ExampleCommand.output({:left, :right}, %{}))
  end

  test "error_string is error" do
    exit_code = exit_software
    assert match?({:error, ^exit_code, "I am string"},
                  ExampleCommand.output({:error_string, "I am string"}, %{}))
  end

  test "error_string is converted to string" do
    exit_code = exit_software
    assert match?({:error, ^exit_code, "I am charlist"},
                  ExampleCommand.output({:error_string, 'I am charlist'}, %{}))
  end

  test "non atom value is ok" do
    val = "foo"
    assert match?({:ok, ^val}, ExampleCommand.output(val, %{}))
    val = 125
    assert match?({:ok, ^val}, ExampleCommand.output(val, %{}))
    val = 100.2
    assert match?({:ok, ^val}, ExampleCommand.output(val, %{}))
    val = {:one, :two, :three}
    assert match?({:ok, ^val}, ExampleCommand.output(val, %{}))
  end

  test "custom output function can be defined" do
    assert match?({:error, 125, "Non standard"},
                  ExampleCommandWithCustomOutput.output(:non_standard_output, %{}))
  end

  test "default output works even if custom output is defined" do
    assert match?(:ok, ExampleCommandWithCustomOutput.output(:ok, %{}))
    assert match?({:ok, {:complex, "message"}},
                  ExampleCommandWithCustomOutput.output({:ok, {:complex, "message"}}, %{}))

    assert match?({:stream, [1,2,3]}, ExampleCommandWithCustomOutput.output({:ok, [1,2,3]}, %{}))
    assert match?({:stream, [1,2,3]}, ExampleCommandWithCustomOutput.output([1,2,3], %{}))

    exit_code = exit_unavailable
    node = :example@node
    assert match?({:error, ^exit_code, "Error: unable to connect to node 'example@node': nodedown"},
                  ExampleCommandWithCustomOutput.output({:badrpc, :nodedown}, %{node: node}))

    exit_code = exit_tempfail
    timeout = 1000
    assert match?({:error, ^exit_code, "Error: operation example_command_with_custom_output on node example@node timed out. Timeout: 1000"},
                  ExampleCommandWithCustomOutput.output({:badrpc, :timeout}, %{timeout: timeout, node: node}))

    exit_code = exit_software
    error = %{i: [am: "arbitrary", error: 1]}
    inspected = inspect(error)
    assert match?({:error, ^exit_code, "Error:\n" <> ^inspected},
                  ExampleCommandWithCustomOutput.output({:error, error}, %{}))

    assert match?({:error, ^exit_code, "I am string"},
                  ExampleCommandWithCustomOutput.output({:error_string, "I am string"}, %{}))

    val = "foo"
    assert match?({:ok, ^val}, ExampleCommandWithCustomOutput.output(val, %{}))
    val = 125
    assert match?({:ok, ^val}, ExampleCommandWithCustomOutput.output(val, %{}))
    val = 100.2
    assert match?({:ok, ^val}, ExampleCommandWithCustomOutput.output(val, %{}))
    val = {:one, :two, :three}
    assert match?({:ok, ^val}, ExampleCommandWithCustomOutput.output(val, %{}))
  end
end

defmodule ExampleCommand do
  use RabbitMQ.CLI.DefaultOutput
end

defmodule ExampleCommandWithCustomOutput do
  def output(:non_standard_output, _) do
    {:error, 125, "Non standard"}
  end
  use RabbitMQ.CLI.DefaultOutput
end
