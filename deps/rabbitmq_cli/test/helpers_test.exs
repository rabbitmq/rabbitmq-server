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


defmodule HelpersTest do
  use ExUnit.Case, async: true
  import TestHelper

  setup_all do
    :net_kernel.start([:rabbitmqctl, :shortnames])
    on_exit([], fn -> :net_kernel.stop() end)
    :ok
  end

  setup context do
    on_exit(context, fn -> :erlang.disconnect_node(context[:target]) end)
    :ok
  end

## --------------------- get_rabbit_hostname/0 tests -------------------------

test "RabbitMQ hostname is properly formed" do
    assert Helpers.get_rabbit_hostname() |> Atom.to_string =~ ~r/rabbit@\w+/
  end

## ------------------- connect_to_rabbitmq/0,1 tests --------------------

  test "RabbitMQ default hostname connects" do
    assert Helpers.connect_to_rabbitmq() == true
  end

  @tag target: get_rabbit_hostname()
  test "RabbitMQ specified hostname atom connects", context do
    assert Helpers.connect_to_rabbitmq(context[:target]) == true
  end

  @tag target: get_rabbit_hostname() |> Atom.to_string
  test "RabbitMQ specified hostname string connects", context do
    assert Helpers.connect_to_rabbitmq(context[:target]) == true
  end

  @tag target: :jake@thedog
  test "Invalid specified hostname atom doesn't connect", context do
    assert Helpers.connect_to_rabbitmq(context[:target]) == false
  end

## ------------------- commands/0 tests --------------------

  test "command_modules has existing commands" do
    assert Helpers.commands["status"] == StatusCommand
    assert Helpers.commands["environment"] == EnvironmentCommand
  end

  test "command_modules does not have non-existent commands" do
    assert Helpers.commands[:p_equals_np_proof] == nil
  end

## ------------------- is_command?/1 tests --------------------

  test "a valid implemented command returns true" do
    assert Helpers.is_command?("status") == true
  end

  test "an invalid command returns false" do
    assert Helpers.is_command?("quack") == false
  end

  test "a nil returns false" do
    assert Helpers.is_command?(nil) == false
  end

  test "an empty array returns true" do
    # An empty command defaults to the help command
    assert Helpers.is_command?([]) == true
  end

  test "an non-empty array tests the first element" do
    assert Helpers.is_command?(["status", "quack"]) == true
    assert Helpers.is_command?(["quack", "status"]) == false
  end

  test "a non-string list returns false" do
    assert Helpers.is_command?([{"status", "quack"}, {4, "Fantastic"}]) == false
  end

## ------------------- memory_unit* tests --------------------

  test "an invalid memory unit fails " do
    assert Helpers.memory_unit_absolute(10, "gigantibytes") == {:bad_argument, ["gigantibytes"]}
  end

  test "an invalid number fails " do
    assert Helpers.memory_unit_absolute("lots", "gigantibytes") == {:bad_argument, ["lots", "gigantibytes"]}
    assert Helpers.memory_unit_absolute(-1, "gigantibytes") == {:bad_argument, [-1, "gigantibytes"]}
  end

  test "valid number and unit returns a valid result  " do
      assert Helpers.memory_unit_absolute(10, "k") == 10240
      assert Helpers.memory_unit_absolute(10, "kiB") == 10240
      assert Helpers.memory_unit_absolute(10, "M") == 10485760
      assert Helpers.memory_unit_absolute(10, "MiB") == 10485760
      assert Helpers.memory_unit_absolute(10, "G") == 10737418240
      assert Helpers.memory_unit_absolute(10, "GiB")== 10737418240
      assert Helpers.memory_unit_absolute(10, "kB")== 10000
      assert Helpers.memory_unit_absolute(10, "MB")== 10000000
      assert Helpers.memory_unit_absolute(10, "GB")== 10000000000
      assert Helpers.memory_unit_absolute(10, "")  == 10
  end


## ------------------- parse_node* tests --------------------

  test "if nil input, retrieve standard rabbit hostname" do
    assert Helpers.parse_node(nil) == get_rabbit_hostname
  end

  test "if input is an atom, return the atom" do
    assert Helpers.parse_node(:rabbit_test) == :rabbit_test
  end

  test "if input is a string fully qualified node name, return an atom" do
    assert Helpers.parse_node("rabbit_test@#{hostname}") == "rabbit_test@#{hostname}" |> String.to_atom
  end

  test "if input is a short node name, host name is added" do
    assert Helpers.parse_node("rabbit_test") == "rabbit_test@#{hostname}" |> String.to_atom
  end

  test "if input is a hostname without a node name, return an atom" do
    assert Helpers.parse_node("@#{hostname}") == "@#{hostname}" |> String.to_atom
  end

  test "if input is a short node name with an @ and no hostname, local host name is added" do
    assert Helpers.parse_node("rabbit_test@") == "rabbit_test@#{hostname}" |> String.to_atom
  end

  test "if input contains more than one @, return atom" do
    assert Helpers.parse_node("rabbit@rabbit_test@#{hostname}") == "rabbit@rabbit_test@#{hostname}" |>String.to_atom
  end
end
