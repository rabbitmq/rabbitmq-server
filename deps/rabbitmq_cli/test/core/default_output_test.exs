## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2020 VMware, Inc. or its affiliates.  All rights reserved.


defmodule DefaultOutputTest do
  use ExUnit.Case, async: false

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

  test "badrpc is an error" do
    {:error, {:badrpc, :nodedown}} =
      ExampleCommand.output({:badrpc, :nodedown}, %{})

    {:error, {:badrpc, :timeout}} =
      ExampleCommand.output({:badrpc, :timeout}, %{})
  end

  test "unknown atom is error" do
    {:error, :error_message} = ExampleCommand.output(:error_message, %{})
  end

  test "unknown tuple is error" do
    {:error, {:left, :right}} = ExampleCommand.output({:left, :right}, %{})
  end

  test "error_string is error" do
    assert {:error, "I am string"} == ExampleCommand.output({:error_string, "I am string"}, %{})
  end

  test "error_string is converted to string" do
    assert match?({:error, "I am charlist"},
                  ExampleCommand.output({:error_string, 'I am charlist'}, %{}))
  end

  test "error is formatted" do
    {:error, "I am formatted \"string\""} =
      ExampleCommand.output({:error, 'I am formatted ~p', ['string']}, %{})
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
    assert {:error, 125, "Non standard"} == ExampleCommandWithCustomOutput.output(:non_standard_output, %{})
  end

  test "default output works even if custom output is defined" do
    assert :ok == ExampleCommandWithCustomOutput.output(:ok, %{})
    assert {:ok, {:complex, "message"}} == ExampleCommandWithCustomOutput.output({:ok, {:complex, "message"}}, %{})

    assert {:stream, [1,2,3]} == ExampleCommandWithCustomOutput.output({:ok, [1,2,3]}, %{})
    assert {:stream, [1,2,3]} == ExampleCommandWithCustomOutput.output([1,2,3], %{})

    assert {:error, {:badrpc, :nodedown}} ==
      ExampleCommandWithCustomOutput.output({:badrpc, :nodedown}, %{})
    assert {:error, {:badrpc, :timeout}} ==
      ExampleCommandWithCustomOutput.output({:badrpc, :timeout}, %{})

    error = %{i: [am: "arbitrary", error: 1]}
    {:error, ^error} = ExampleCommandWithCustomOutput.output({:error, error}, %{})

    assert {:error, "I am string"} == ExampleCommandWithCustomOutput.output({:error_string, "I am string"}, %{})

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
