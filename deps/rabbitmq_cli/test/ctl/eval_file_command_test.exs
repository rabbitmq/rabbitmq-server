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


defmodule EvalFileCommandTest do
  use ExUnit.Case, async: false
  import TestHelper
  import ExUnit.CaptureIO

  @command RabbitMQ.CLI.Ctl.Commands.EvalFileCommand

  setup_all do
    RabbitMQ.CLI.Core.Distribution.start()
    :ok
  end

  setup _ do
    {:ok, opts: %{node: get_rabbit_hostname()}}
  end

  test "validate: providing no arguments fails validation" do
    # expression is expected to be provided via standard input
    assert @command.validate([], %{}) == {:validation_failure, :not_enough_args}
  end

  test "validate: empty file path fails validation" do
    assert @command.validate([""], %{}) == {:validation_failure, "File path must not be blank"}
  end

  test "validate: path to a non-existent file fails validation" do
    path = "/tmp/rabbitmq/cli-tests/12937293782368263726.lolz.escript"
    assert @command.validate([path], %{}) == {:validation_failure, "File #{path} does not exist"}
  end

  test "run: request to a non-existent node returns a badrpc", _context do
    opts = %{node: :jake@thedog, timeout: 200}

    assert match?({:badrpc, _}, @command.run([valid_file_path()], opts))
  end

  test "run: evaluates expressions in the file on the target server node", context do
    {:ok, apps} = @command.run([loaded_applications_file_path()], context[:opts])
    assert is_list(apps)
    assert List.keymember?(apps, :rabbit, 0)
  end

  test "run: returns evaluation result", context do
    assert {:ok, 2} == @command.run([valid_file_path()], context[:opts])
  end

  test "run: reports invalid syntax errors", context do
    assert match?({:error, _}, @command.run([invalid_file_path()], context[:opts]))
  end

  #
  # Implementation
  #

  defp valid_file_path() do
    Path.join([File.cwd!(), "test", "fixtures", "files", "valid_erl_expression.escript"])
  end

  defp invalid_file_path() do
    Path.join([File.cwd!(), "test", "fixtures", "files", "invalid_erl_expression.escript"])
  end

  defp loaded_applications_file_path() do
    Path.join([File.cwd!(), "test", "fixtures", "files", "loaded_applications.escript"])
  end
end
