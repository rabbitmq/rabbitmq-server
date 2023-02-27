## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2018-2020 VMware, Inc. or its affiliates.  All rights reserved.

defmodule ListFeatureFlagsCommandTest do
  use ExUnit.Case, async: false
  import TestHelper

  @command RabbitMQ.CLI.Ctl.Commands.ListFeatureFlagsCommand

  @flag1 :ff1_from_list_ff_testsuite
  @flag2 :ff2_from_list_ff_testsuite

  setup_all do
    RabbitMQ.CLI.Core.Distribution.start()

    # Define an arbitrary feature flag for the test.
    node = get_rabbit_hostname()

    new_feature_flags = %{
      @flag1 => %{
        desc: 'My feature flag #1',
        provided_by: :ListFeatureFlagsCommandTest,
        stability: :stable
      },
      @flag2 => %{
        desc: 'My feature flag #2',
        provided_by: :ListFeatureFlagsCommandTest,
        stability: :stable
      }
    }

    :ok =
      :rabbit_misc.rpc_call(
        node,
        :rabbit_feature_flags,
        :inject_test_feature_flags,
        [new_feature_flags]
      )

    :ok =
      :rabbit_misc.rpc_call(
        node,
        :rabbit_feature_flags,
        :enable_all,
        []
      )

    name_result = [
      [{:name, @flag1}],
      [{:name, @flag2}]
    ]

    full_result = [
      [{:name, @flag1}, {:state, :enabled}],
      [{:name, @flag2}, {:state, :enabled}]
    ]

    {
      :ok,
      name_result: name_result, full_result: full_result
    }
  end

  setup context do
    {
      :ok,
      opts: %{node: get_rabbit_hostname(), timeout: context[:test_timeout]}
    }
  end

  test "merge_defaults with no command, print just use the names" do
    assert match?({["name", "state"], %{}}, @command.merge_defaults([], %{}))
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
    assert @command.validate(["quack", "name"], context[:opts]) ==
             {:validation_failure, {:bad_info_key, [:quack]}}

    assert @command.validate(["name", "oink"], context[:opts]) ==
             {:validation_failure, {:bad_info_key, [:oink]}}

    assert @command.validate(["name", "oink", "state"], context[:opts]) ==
             {:validation_failure, {:bad_info_key, [:oink]}}
  end

  test "run: on a bad RabbitMQ node, return a badrpc" do
    opts = %{node: :jake@thedog, timeout: 200}
    assert match?({:badrpc, _}, @command.run(["name"], opts))
  end

  @tag test_timeout: :infinity
  test "run: with the name tag, print just the names", context do
    matches_found = @command.run(["name"], context[:opts])

    assert Enum.all?(context[:name_result], fn feature_name ->
             Enum.find(matches_found, fn found -> found == feature_name end)
           end)
  end

  @tag test_timeout: :infinity
  test "run: duplicate args do not produce duplicate entries", context do
    # checks to ensure that all expected feature flags are in the results
    matches_found = @command.run(["name", "name"], context[:opts])

    assert Enum.all?(context[:name_result], fn feature_name ->
             Enum.find(matches_found, fn found -> found == feature_name end)
           end)
  end

  @tag test_timeout: 30000
  test "run: sufficiently long timeouts don't interfere with results", context do
    matches_found = @command.run(["name", "state"], context[:opts])

    assert Enum.all?(context[:full_result], fn feature_name ->
             Enum.find(matches_found, fn found -> found == feature_name end)
           end)
  end

  @tag test_timeout: 0, username: "guest"
  test "run: timeout causes command to return a bad RPC", context do
    assert @command.run(["name", "state"], context[:opts]) ==
             {:badrpc, :timeout}
  end

  @tag test_timeout: :infinity
  test "banner", context do
    assert @command.banner([], context[:opts]) =~ ~r/Listing feature flags \.\.\./
  end
end
