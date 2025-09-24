## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.

defmodule MessageSizeStatsCommandTest do
  use ExUnit.Case, async: false
  import TestHelper

  @command RabbitMQ.CLI.Diagnostics.Commands.MessageSizeStatsCommand

  setup_all do
    RabbitMQ.CLI.Core.Distribution.start()

    :ok
  end

  setup do
    {:ok, opts: %{node: get_rabbit_hostname(), timeout: 60_000}}
  end

  test "validate: with extra arguments returns an arg count error", context do
    assert @command.validate(["extra"], context[:opts]) == {:validation_failure, :too_many_args}
  end

  test "validate: with no arguments succeeds", context do
    assert @command.validate([], context[:opts]) == :ok
  end

  test "run: request to a named, active node succeeds", context do
    result = @command.run([], context[:opts])
    Enum.each(result, fn row ->
      assert is_list(row)
      assert Enum.any?(row, fn {key, _} -> key == "Message Size" end)
      assert Enum.any?(row, fn {key, _} -> key == "Count" end)
      assert Enum.any?(row, fn {key, _} -> key == "Percentage" end)
      count_value = Enum.find_value(row, fn {key, value} -> if key == "Count", do: value end)
      percentage_value = Enum.find_value(row, fn {key, value} -> if key == "Percentage", do: value end)
      assert is_integer(count_value)
      assert is_float(String.to_float(percentage_value))
    end)
  end

  test "run: request to a non-existent node returns a badrpc" do
    opts = %{node: :jake@thedog, timeout: 200}
    assert match?({:badrpc, _}, @command.run([], opts))
  end

  test "banner", context do
    banner = @command.banner([], context[:opts])
    assert banner =~ ~r/Gathering message size statistics from cluster via #{get_rabbit_hostname()}/
  end

end
