## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2023 Broadcom. All Rights Reserved. The term “Broadcom”
## refers to Broadcom Inc. and/or its subsidiaries.  All rights reserved.

defmodule ListDeprecatedFeaturesCommandTest do
  use ExUnit.Case, async: false
  import TestHelper

  @command RabbitMQ.CLI.Ctl.Commands.ListDeprecatedFeaturesCommand

  @df1 :df1_from_list_df_testsuite
  @df2 :df2_from_list_df_testsuite

  setup_all do
    RabbitMQ.CLI.Core.Distribution.start()

    # Define an arbitrary deprecated feature for the test.
    node = get_rabbit_hostname()

    new_deprecated_features = %{
      @df1 => %{
        desc: ~c"My deprecated feature #1",
        provided_by: :ListDeprecatedFeaturesCommandTest,
        deprecation_phase: :permitted_by_default
      },
      @df2 => %{
        desc: ~c"My deprecated feature #2",
        provided_by: :ListDeprecatedFeaturesCommandTest,
        deprecation_phase: :removed
      }
    }

    :ok =
      :rabbit_misc.rpc_call(
        node,
        :rabbit_feature_flags,
        :inject_test_feature_flags,
        [new_deprecated_features]
      )

    name_result = [
      [{:name, @df1}],
      [{:name, @df2}]
    ]

    full_result = [
      [{:name, @df1}, {:deprecation_phase, :permitted_by_default}],
      [{:name, @df2}, {:deprecation_phase, :removed}]
    ]

    {
      :ok,
      name_result: name_result, full_result: full_result
    }
  end

  setup context do
    {
      :ok,
      opts: %{node: get_rabbit_hostname(), timeout: context[:test_timeout], used: false}
    }
  end

  test "merge_defaults with no command, print just use the names" do
    assert match?({["name", "deprecation_phase"], %{}}, @command.merge_defaults([], %{}))
  end

  test "validate: return bad_info_key on a single bad arg", context do
    assert @command.validate(["quack"], context[:opts]) ==
             {:validation_failure, {:bad_info_key, [:quack]}}
  end

  test "validate: returns multiple bad args return a list of bad info key values", context do
    result = @command.validate(["quack", "oink"], context[:opts])
    assert match?({:validation_failure, {:bad_info_key, _}}, result)
    {_, {_, keys}} = result
    assert :lists.sort(keys) == [:oink, :quack]
  end

  test "validate: return bad_info_key on mix of good and bad args", context do
    assert @command.validate(["quack", "name"], context[:opts]) ==
             {:validation_failure, {:bad_info_key, [:quack]}}

    assert @command.validate(["name", "oink"], context[:opts]) ==
             {:validation_failure, {:bad_info_key, [:oink]}}

    assert @command.validate(["name", "oink", "deprecation_phase"], context[:opts]) ==
             {:validation_failure, {:bad_info_key, [:oink]}}
  end

  test "run: on a bad RabbitMQ node, return a badrpc" do
    opts = %{node: :jake@thedog, timeout: 200, used: false}
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
  test "run: with the name tag, print just the names for used features", context do
    opts = %{node: get_rabbit_hostname(), timeout: context[:test_timeout], used: true}
    matches_found = @command.run(["name"], opts)

    assert Enum.empty?(matches_found)
  end

  @tag test_timeout: :infinity
  test "run: duplicate args do not produce duplicate entries", context do
    # checks to ensure that all expected deprecated features are in the results
    matches_found = @command.run(["name", "name"], context[:opts])

    assert Enum.all?(context[:name_result], fn feature_name ->
             Enum.find(matches_found, fn found -> found == feature_name end)
           end)
  end

  @tag test_timeout: 30000
  test "run: sufficiently long timeouts don't interfere with results", context do
    matches_found = @command.run(["name", "deprecation_phase"], context[:opts])

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
    assert @command.banner([], context[:opts]) =~ ~r/Listing deprecated features \.\.\./
  end
end
