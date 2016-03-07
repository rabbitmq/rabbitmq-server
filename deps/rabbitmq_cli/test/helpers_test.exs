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
    :net_kernel.connect_node(context[:target])
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

  @tag target: :jake@thedog
  test "Invalid specified hostname atom doesn't connect", context do
    assert Helpers.connect_to_rabbitmq(context[:target]) == false
  end

## ------------------- commands/0 tests --------------------

  test "command_modules has existing commands" do
    assert Helpers.commands["status"] == "StatusCommand"
    assert Helpers.commands["environment"] == "EnvironmentCommand"
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

  test "an empty array returns false" do
    assert Helpers.is_command?([]) == false
  end

  test "an non-empty array tests the first element" do
    assert Helpers.is_command?(["status", "quack"]) == true
    assert Helpers.is_command?(["quack", "status"]) == false
  end

  test "a non-string list returns false" do
    assert Helpers.is_command?([{"status", "quack"}, {4, "Fantastic"}]) == false
  end
end
