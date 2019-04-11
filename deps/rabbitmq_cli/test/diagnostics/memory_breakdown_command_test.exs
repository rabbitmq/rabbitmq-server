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
## The Initial Developer of the Original Code is Pivotal Software, Inc.
## Copyright (c) 2016-2017 Pivotal Software, Inc.  All rights reserved.

defmodule MemoryBreakdownCommandTest do
  use ExUnit.Case
  import TestHelper

  @command RabbitMQ.CLI.Diagnostics.Commands.MemoryBreakdownCommand

  setup_all do
    RabbitMQ.CLI.Core.Distribution.start()


    start_rabbitmq_app()

    on_exit([], fn ->
      start_rabbitmq_app()
    end)

    :ok
  end

  setup do
    {:ok, opts: %{
      node: get_rabbit_hostname(),
      timeout: 5000,
      unit: "gb"
    }}
  end

  test "validate: specifying a positional argument fails validation", context do
    assert @command.validate(["abc"], context[:opts]) ==
    {:validation_failure, :too_many_args}

    assert @command.validate(["abc", "def"], context[:opts]) ==
      {:validation_failure, :too_many_args}
  end

  test "validate: specifying no positional arguments and no options succeeds", context do
    assert @command.validate([], context[:opts]) == :ok
  end

  test "validate: specifying gigabytes as a --unit succeeds", context do
    assert @command.validate([], Map.merge(context[:opts], %{unit: "gb"})) == :ok
  end

  test "validate: specifying bytes as a --unit succeeds", context do
    assert @command.validate([], Map.merge(context[:opts], %{unit: "bytes"})) == :ok
  end

  test "validate: specifying megabytes as a --unit succeeds", context do
    assert @command.validate([], Map.merge(context[:opts], %{unit: "mb"})) == :ok
  end

  test "validate: specifying glip-glops as a --unit fails validation", context do
    assert @command.validate([], Map.merge(context[:opts], %{unit: "glip-glops"})) ==
    {:validation_failure, "unit 'glip-glops' is not supported. Please use one of: bytes, mb, gb"}
  end

  test "run: request to a non-existent RabbitMQ node returns a nodedown" do
    opts = %{node: :jake@thedog, timeout: 200, unit: "gb"}
    assert match?({:badrpc, _}, @command.run([], opts))
  end

  test "banner", context do
    s = @command.banner([], context[:opts])

    assert s =~ ~r/Reporting memory breakdown on node/
  end
end
