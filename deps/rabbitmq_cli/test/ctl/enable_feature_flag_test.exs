## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2024 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.

defmodule EnableFeatureFlagCommandTest do
  use ExUnit.Case, async: false
  import TestHelper

  @command RabbitMQ.CLI.Ctl.Commands.EnableFeatureFlagCommand
  @feature_flag :ff_from_enable_ff_testsuite
  @experimental_flag :ff_from_enable_ff_testsuite_experimental
  @usage_exit_code RabbitMQ.CLI.Core.ExitCodes.exit_usage()

  setup_all do
    RabbitMQ.CLI.Core.Distribution.start()

    # Define an arbitrary feature flag for the test.
    node = get_rabbit_hostname()

    new_feature_flags = %{
      @feature_flag => %{
        desc: ~c"My feature flag",
        provided_by: :EnableFeatureFlagCommandTest,
        stability: :stable
      },
      @experimental_flag => %{
        desc: ~c"An **experimental** feature!",
        provided_by: :EnableFeatureFlagCommandTest,
        stability: :experimental
      }
    }

    :ok =
      :rabbit_misc.rpc_call(
        node,
        :rabbit_feature_flags,
        :inject_test_feature_flags,
        [new_feature_flags]
      )

    {
      :ok,
      opts: %{node: get_rabbit_hostname(), experimental: false},
      feature_flag: @feature_flag,
      experimental_flag: @experimental_flag
    }
  end

  test "validate: wrong number of arguments results in arg count errors" do
    assert @command.validate([], %{}) == {:validation_failure, :not_enough_args}

    assert @command.validate(["ff_from_enable_ff_testsuite", "whoops"], %{}) ==
             {:validation_failure, :too_many_args}
  end

  test "validate: passing an empty string for feature_flag name is an arg error", context do
    assert match?(
             {:validation_failure, {:bad_argument, _}},
             @command.validate([""], context[:opts])
           )
  end

  test "run: passing a valid feature_flag name to a running RabbitMQ node succeeds", context do
    assert @command.run([Atom.to_string(context[:feature_flag])], context[:opts]) == :ok
    assert list_feature_flags(:enabled) |> Map.has_key?(context[:feature_flag])
  end

  test "run: attempt to use an unreachable node with --opt-in returns a nodedown" do
    opts = %{node: :jake@thedog, timeout: 200, opt_in: false}
    assert match?({:badrpc, _}, @command.run(["na"], opts))
  end

  test "run: attempt to use an unreachable node with --experimental returns a nodedown" do
    opts = %{node: :jake@thedog, timeout: 200, experimental: false}
    assert match?({:badrpc, _}, @command.run(["na"], opts))
  end

  test "run: enabling an experimental flag requires '--opt-in'", context do
    experimental_flag = Atom.to_string(context[:experimental_flag])
    assert match?(
             {:error, @usage_exit_code, _},
             @command.run([experimental_flag], context[:opts])
           )
    opts = Map.put(context[:opts], :opt_in, true)
    assert @command.run([experimental_flag], opts) == :ok
  end

  test "run: enabling an experimental flag accepts '--experimental'", context do
    experimental_flag = Atom.to_string(context[:experimental_flag])
    assert match?(
             {:error, @usage_exit_code, _},
             @command.run([experimental_flag], context[:opts])
           )
    opts = Map.put(context[:opts], :experimental, true)
    assert @command.run([experimental_flag], opts) == :ok
  end

  test "run: enabling the same feature flag twice is idempotent", context do
    enable_feature_flag(context[:feature_flag])
    assert @command.run([Atom.to_string(context[:feature_flag])], context[:opts]) == :ok
    assert list_feature_flags(:enabled) |> Map.has_key?(context[:feature_flag])
  end

  test "run: enabling all feature flags succeeds", context do
    enable_feature_flag(context[:feature_flag])
    assert @command.run(["all"], context[:opts]) == :ok
    assert list_feature_flags(:enabled) |> Map.has_key?(context[:feature_flag])
  end

  test "run: enabling all feature flags with '--opt-in' returns an error", context do
    enable_feature_flag(context[:feature_flag])
    opts = Map.put(context[:opts], :opt_in, true)
    assert match?({:error, @usage_exit_code, _}, @command.run(["all"], opts))
  end

  test "run: enabling all feature flags with '--experimental' returns an error", context do
    enable_feature_flag(context[:feature_flag])
    opts = Map.put(context[:opts], :experimental, true)
    assert match?({:error, @usage_exit_code, _}, @command.run(["all"], opts))
  end

  test "banner", context do
    assert @command.banner([context[:feature_flag]], context[:opts]) =~
             ~r/Enabling feature flag \"#{context[:feature_flag]}\" \.\.\./
  end
end
