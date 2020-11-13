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


defmodule EvalCommandTest do
  use ExUnit.Case, async: false
  import TestHelper
  import ExUnit.CaptureIO

  @command RabbitMQ.CLI.Ctl.Commands.EvalCommand

  setup_all do
    RabbitMQ.CLI.Core.Distribution.start()
    :ok
  end

  setup _ do
    {:ok, opts: %{node: get_rabbit_hostname()}}
  end

  test "validate: providing too few arguments fails validation" do
    assert @command.validate([], %{}) == {:validation_failure, :not_enough_args}
  end

  test "validate: empty expression to eval fails validation" do
    assert @command.validate([""], %{}) == {:validation_failure, "Expression must not be blank"}
    assert @command.validate(["", "foo"], %{}) == {:validation_failure, "Expression must not be blank"}
  end

  test "validate: syntax error in expression to eval fails validation" do
    assert @command.validate(["foo bar"], %{}) == {:validation_failure, "syntax error before: bar"}
    assert @command.validate(["foo bar", "foo"], %{}) == {:validation_failure, "syntax error before: bar"}
  end

  test "run: request to a non-existent node returns a badrpc", _context do
    opts = %{node: :jake@thedog, timeout: 200}

    assert match?({:badrpc, _}, @command.run(["ok."], opts))
  end

  test "run: evaluates provided Erlang expression", context do
    assert @command.run(["foo."], context[:opts]) == {:ok, :foo}
    assert @command.run(["length([1,2,3])."], context[:opts]) == {:ok, 3}
    assert @command.run(["lists:sum([1,2,3])."], context[:opts]) == {:ok, 6}
    {:ok, apps} = @command.run(["application:loaded_applications()."], context[:opts])
    assert is_list(apps)
  end

  test "run: evaluates provided expression on the target server node", context do
    {:ok, apps} = @command.run(["application:loaded_applications()."], context[:opts])
    assert is_list(apps)
    assert List.keymember?(apps, :rabbit, 0)
  end

  test "run: returns stdout output", context do
    assert capture_io(fn ->
      assert @command.run(["io:format(\"output\")."], context[:opts]) == {:ok, :ok}
    end) == "output"
  end

  test "run: passes parameters to the expression as positional/numerical variables", context do
    assert @command.run(["binary_to_atom(_1, utf8).", "foo"], context[:opts]) == {:ok, :foo}
    assert @command.run(["{_1, _2}.", "foo", "bar"], context[:opts]) == {:ok, {"foo", "bar"}}
  end

  test "run: passes globally recognised options as named variables", context do
    assert @command.run(["{_vhost, _node}."], Map.put(context[:opts], :vhost, "a-node")) ==
      {:ok, {"a-node", context[:opts][:node]}}
  end
end
