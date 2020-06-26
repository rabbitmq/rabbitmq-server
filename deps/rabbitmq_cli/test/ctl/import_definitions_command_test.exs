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
## Copyright (c) 2007-2020 VMware, Inc. or its affiliates.  All rights reserved.


defmodule ImportDefinitionsCommandTest do
  use ExUnit.Case, async: false
  import TestHelper

  @command RabbitMQ.CLI.Ctl.Commands.ImportDefinitionsCommand

  setup_all do
    RabbitMQ.CLI.Core.Distribution.start()

    :ok
  end

  setup context do
    {:ok, opts: %{
        node: get_rabbit_hostname(),
        timeout: context[:test_timeout] || 30000,
        format: context[:format] || "json"
      }}
  end

  test "merge_defaults: defaults to JSON for format" do
    assert @command.merge_defaults([valid_file_path()], %{}) ==
             {[valid_file_path()], %{format: "json"}}
  end

  test "merge_defaults: defaults to --silent if target is stdout" do
    assert @command.merge_defaults(["-"], %{}) == {["-"], %{format: "json", silent: true}}
  end

  test "merge_defaults: format is case insensitive" do
    assert @command.merge_defaults([valid_file_path()], %{format: "JSON"}) ==
             {[valid_file_path()], %{format: "json"}}
    assert @command.merge_defaults([valid_file_path()], %{format: "Erlang"}) ==
             {[valid_file_path()], %{format: "erlang"}}
  end

  test "merge_defaults: format can be overridden" do
    assert @command.merge_defaults([valid_file_path()], %{format: "erlang"}) ==
             {[valid_file_path()], %{format: "erlang"}}
  end

  test "validate: accepts a file path argument", context do
    assert @command.validate([valid_file_path()], context[:opts]) == :ok
  end

  test "validate: unsupported format fails validation", context do
    assert match?({:validation_failure, {:bad_argument, _}},
                  @command.validate([valid_file_path()], Map.merge(context[:opts], %{format: "yolo"})))
  end

  test "validate: more than one positional argument fails validation", context do
    assert @command.validate([valid_file_path(), "extra-arg"], context[:opts]) ==
             {:validation_failure, :too_many_args}
  end

  test "validate: supports JSON and Erlang formats", context do
    assert @command.validate([valid_file_path()], Map.merge(context[:opts], %{format: "json"})) == :ok
    assert @command.validate([valid_file_path()], Map.merge(context[:opts], %{format: "erlang"})) == :ok
  end

  @tag test_timeout: 3000
  test "run: targeting an unreachable node throws a badrpc", context do
    result = @command.run([valid_file_path()],
                          %{node: :jake@thedog,
                            timeout: context[:test_timeout],
                            format: "json"})
    assert match?({:badrpc, _}, result)
  end

  @tag format: "json"
  test "run: imports definitions from a file", context do
    assert :ok == @command.run([valid_file_path()], context[:opts])

    # clean up the state we've modified
    clear_parameter("/", "federation-upstream", "up-1")
  end

  defp valid_file_path() do
    Path.join([File.cwd!(), "test", "fixtures", "files", "definitions.json"])
  end
end
