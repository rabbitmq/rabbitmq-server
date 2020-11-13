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


defmodule ExecCommandTest do
  use ExUnit.Case, async: false

  @command RabbitMQ.CLI.Ctl.Commands.ExecCommand

  setup _ do
    {:ok, opts: %{}}
  end

  test "validate: providing too few arguments fails validation" do
    assert @command.validate([], %{}) == {:validation_failure, :not_enough_args}
  end

  test "validate: there should be only one argument" do
    assert @command.validate(["foo", "bar"], %{}) == {:validation_failure, :too_many_args}
    assert @command.validate(["", "bar"], %{}) == {:validation_failure, :too_many_args}
  end

  test "validate: empty expression to exec fails validation" do
    assert @command.validate([""], %{}) == {:validation_failure, "Expression must not be blank"}
  end

  test "validate: success" do
    :ok = @command.validate([":ok"], %{})
  end

  test "run: executes elixir code" do
    {:ok, :ok} = @command.run([":ok"], %{})
    node = Node.self()
    {:ok, ^node} = @command.run(["Node.self()"], %{})
    {:ok, 3} = @command.run(["1 + 2"], %{})
  end

  test "run: binds options variable" do
    opts = %{my: :custom, option: 123}
    {:ok, ^opts} = @command.run(["options"], opts)
    {:ok, 123} = @command.run(["options[:option]"], opts)
  end

end
