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


defmodule WaitCommandTest do
  use ExUnit.Case, async: false
  import TestHelper

  @command RabbitMQ.CLI.Ctl.Commands.WaitCommand

  setup_all do
    RabbitMQ.CLI.Core.Distribution.start()
    :net_kernel.connect_node(get_rabbit_hostname)

    on_exit([], fn ->
      :erlang.disconnect_node(get_rabbit_hostname)

    end)

    :ok
  end

  setup do
    {:ok, opts: %{node: get_rabbit_hostname}}
  end

  test "validate: with extra arguments returns an arg count error", context do
    assert @command.validate(["pid_file", "extra"], context[:opts]) == {:validation_failure, :too_many_args}
    assert @command.validate([], context[:opts]) == {:validation_failure, :not_enough_args}
  end

  test "banner", context do
    assert @command.banner([], context[:opts]) =~ ~r/Waiting for node #{get_rabbit_hostname}/
  end

  test "output: process not running error", context do
    exit_code = RabbitMQ.CLI.Core.ExitCodes.exit_software
    assert match?({:error, ^exit_code, "Error: process is not running."},
                  @command.output({:error, :process_not_running}, context[:opts]))
  end

  test "output: garbage in pid file error", context do
    exit_code = RabbitMQ.CLI.Core.ExitCodes.exit_software
    assert match?({:error, ^exit_code, "Error: garbage in pid file."},
                  @command.output({:error, {:garbage_in_pid_file, "somefile"}}, context[:opts]))
  end

  test "output: could not read pid error", context do
    exit_code = RabbitMQ.CLI.Core.ExitCodes.exit_software
    assert match?({:error, ^exit_code, "Error: could not read pid. Detail: something wrong"},
                  @command.output({:error, {:could_not_read_pid, "something wrong"}}, context[:opts]))
  end

  test "output: default output is fine", context do
    exit_code = RabbitMQ.CLI.Core.ExitCodes.exit_software
    assert match?({:error, ^exit_code, "Error:\nmessage"}, @command.output({:error, "message"}, context[:opts]))
    assert match?({:error, ^exit_code, "Error:\nmessage"}, @command.output({:error, :message}, context[:opts]))
    assert match?({:error, ^exit_code, "Error:\nmessage"}, @command.output(:message, context[:opts]))
    assert match?({:ok, "ok"}, @command.output({:ok, "ok"}, context[:opts]))
    assert match?(:ok, @command.output(:ok, context[:opts]))
    assert match?({:ok, "ok"}, @command.output("ok", context[:opts]))
  end
end
