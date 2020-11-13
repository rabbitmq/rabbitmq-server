## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2020 VMware, Inc. or its affiliates.  All rights reserved.


defmodule ListVhostsCommandTest do
  use ExUnit.Case, async: false
  import TestHelper

  @command RabbitMQ.CLI.Ctl.Commands.ListVhostsCommand

  @vhost1 "test1"
  @vhost2 "test2"
  @root   "/"

  setup_all do
    RabbitMQ.CLI.Core.Distribution.start()

    add_vhost @vhost1
    add_vhost @vhost2
    trace_off @root

    on_exit([], fn ->
      delete_vhost @vhost1
      delete_vhost @vhost2
    end)

    name_result = [
      [{:name, @vhost1}],
      [{:name, @vhost2}],
      [{:name, @root}]
    ]

    tracing_result = [
      [{:tracing, false}],
      [{:tracing, false}],
      [{:tracing, false}]
    ]

    full_result = [
      [{:name, @vhost1}, {:tracing, false}],
      [{:name, @vhost2}, {:tracing, false}],
      [{:name, @root}, {:tracing, false}]
    ]

    transposed_result = [
      [{:tracing, false}, {:name, @vhost1}],
      [{:tracing, false}, {:name, @vhost2}],
      [{:tracing, false}, {:name, @root}]
    ]

    {
      :ok,
      name_result: name_result,
      tracing_result: tracing_result,
      full_result: full_result,
      transposed_result: transposed_result
    }
  end

  setup context do
    {
      :ok,
      opts: %{node: get_rabbit_hostname(), timeout: context[:test_timeout]}
    }
  end

  test "merge_defaults with no command, print just use the names" do
    assert match?({["name"], %{}}, @command.merge_defaults([], %{}))
  end

  test "validate: return bad_info_key on a single bad arg", context do
    assert @command.validate(["quack"], context[:opts]) ==
      {:validation_failure, {:bad_info_key, [:quack]}}
  end

  test "validate: multiple bad args return a list of bad info key values", context do
    assert @command.validate(["quack", "oink"], context[:opts]) ==
      {:validation_failure, {:bad_info_key, [:oink, :quack]}}
  end

  test "validate: return bad_info_key on mix of good and bad args", context do
    assert @command.validate(["quack", "tracing"], context[:opts]) ==
      {:validation_failure, {:bad_info_key, [:quack]}}
    assert @command.validate(["name", "oink"], context[:opts]) ==
      {:validation_failure, {:bad_info_key, [:oink]}}
    assert @command.validate(["name", "oink", "tracing"], context[:opts]) ==
      {:validation_failure, {:bad_info_key, [:oink]}}
  end

  test "run: on a bad RabbitMQ node, return a badrpc" do
    opts = %{node: :jake@thedog, timeout: 200}

    assert match?({:badrpc, _}, @command.run(["name"], opts))
  end

  @tag test_timeout: :infinity
  test "run: with the name tag, print just the names", context do
    # checks to ensure that all expected vhosts are in the results
    matches_found = @command.run(["name"], context[:opts])
    assert Enum.all?(context[:name_result], fn(vhost) ->
      Enum.find(matches_found, fn(found) -> found == vhost end)
    end)
  end

  @tag test_timeout: :infinity
  test "run: with the tracing tag, print just say if tracing is on", context do
    # checks to ensure that all expected vhosts are in the results
    matches_found = @command.run(["tracing"], context[:opts])
    assert Enum.all?(context[:tracing_result], fn(vhost) ->
      Enum.find(matches_found, fn(found) -> found == vhost end)
    end)
  end

  @tag test_timeout: :infinity
  test "run: with name and tracing keys, print both", context do
    # checks to ensure that all expected vhosts are in the results
    matches_found = @command.run(["name", "tracing"], context[:opts])
    assert Enum.all?(context[:full_result], fn(vhost) ->
      Enum.find(matches_found, fn(found) -> found == vhost end)
    end)

    # checks to ensure that all expected vhosts are in the results
    matches_found = @command.run(["tracing", "name"], context[:opts])
    assert Enum.all?(context[:transposed_result], fn(vhost) ->
      Enum.find(matches_found, fn(found) -> found == vhost end)
    end)
  end

  @tag test_timeout: :infinity
  test "run: duplicate args do not produce duplicate entries", context do
    # checks to ensure that all expected vhosts are in the results
    matches_found = @command.run(["name", "name"], context[:opts])
    assert Enum.all?(context[:name_result], fn(vhost) ->
      Enum.find(matches_found, fn(found) -> found == vhost end)
    end)
  end

  @tag test_timeout: 30000
  test "run: sufficiently long timeouts don't interfere with results", context do
    # checks to ensure that all expected vhosts are in the results
    matches_found = @command.run(["name", "tracing"], context[:opts])
    assert Enum.all?(context[:full_result], fn(vhost) ->
      Enum.find(matches_found, fn(found) -> found == vhost end)
    end)
  end

  @tag test_timeout: 0, username: "guest"
  test "run: timeout causes command to return a bad RPC", context do
    assert @command.run(["name", "tracing"], context[:opts]) ==
      {:badrpc, :timeout}
  end

  @tag test_timeout: :infinity
  test "banner", context do
    assert @command.banner([], context[:opts]) =~ ~r/Listing vhosts \.\.\./
  end
end
