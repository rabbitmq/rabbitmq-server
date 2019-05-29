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

defmodule CheckPortListenerCommandTest do
  use ExUnit.Case
  import TestHelper

  @command RabbitMQ.CLI.Diagnostics.Commands.CheckPortListenerCommand

  setup_all do
    RabbitMQ.CLI.Core.Distribution.start()

    :ok
  end

  setup context do
    {:ok, opts: %{
        node: get_rabbit_hostname(),
        timeout: context[:test_timeout] || 30000
      }}
  end

  test "merge_defaults: nothing to do" do
    assert @command.merge_defaults([], %{}) == {[], %{}}
  end

  test "validate: when no arguments are provided, returns a failure" do
    assert @command.validate([], %{}) == {:validation_failure, :not_enough_args}
  end

  test "validate: when two or more arguments are provided, returns a failure" do
    assert @command.validate([5672, 61613], %{}) == {:validation_failure, :too_many_args}
  end

  test "validate: treats a single positional argument and default switches as a success" do
    assert @command.validate([1883], %{}) == :ok
  end

  @tag test_timeout: 3000
  test "run: targeting an unreachable node throws a badrpc", context do
    assert match?({:badrpc, _}, @command.run([61613], Map.merge(context[:opts], %{node: :jake@thedog})))
  end

  test "run: when a listener for the protocol is active, returns a success", context do
    assert match?({true, _}, @command.run([5672], context[:opts]))
  end

  test "run: when a listener on the port is not active or unknown, returns an error", context do
    assert match?({false, _, _}, @command.run([47777], context[:opts]))
  end

  test "output: when a listener for the port is active, returns a success", context do
    assert match?({:ok, _}, @command.output({true, 5672}, context[:opts]))
  end

  test "output: when a listener for the port is not active, returns an error", context do
    assert match?({:error, _, _}, @command.output({false, 15672, []}, context[:opts]))
  end
end
