## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2020 VMware, Inc. or its affiliates.  All rights reserved.


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
