## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2020 VMware, Inc. or its affiliates.  All rights reserved.


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

  test "validate: providing no arguments succeeds" do
    # expression is expected to be provided via standard input
    assert @command.validate([], %{}) == :ok
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
