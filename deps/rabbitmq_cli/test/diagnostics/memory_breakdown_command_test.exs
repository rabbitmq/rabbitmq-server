## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2016-2020 VMware, Inc. or its affiliates.  All rights reserved.

defmodule MemoryBreakdownCommandTest do
  use ExUnit.Case, async: false
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
