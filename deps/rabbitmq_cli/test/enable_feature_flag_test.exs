## The contents of this file are subject to the Mozilla Public License
## Version 1.1 (the "License"); you may not use this file except in
## compliance with the License. You may obtain a copy of the License
## at http://www.mozilla.org/MPL/
##
## Software distributed under the License is distributed on an "AS IS"
## basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
## the License for the specific language governing rights and
## limitations under the License.
##
## The Original Code is RabbitMQ.
##
## The Initial Developer of the Original Code is GoPivotal, Inc.
## Copyright (c) 2018-2019 Pivotal Software, Inc.  All rights reserved.


defmodule EnableFeatureFlagCommandTest do
  use ExUnit.Case, async: false
  import TestHelper

  @command RabbitMQ.CLI.Ctl.Commands.EnableFeatureFlagCommand
  @feature_flag :ff_from_enable_ff_testsuite

  setup_all do
    RabbitMQ.CLI.Core.Distribution.start()

    # Define an arbitrary feature flag for the test.
    node = get_rabbit_hostname()
    new_feature_flags = %{
      @feature_flag =>
      %{desc: "My feature flag",
        provided_by: :EnableFeatureFlagCommandTest,
        stability: :stable}}
    :ok = :rabbit_misc.rpc_call(
      node, :rabbit_feature_flags, :initialize_registry, [new_feature_flags])

    {
      :ok,
      opts: %{node: get_rabbit_hostname()},
      feature_flag: @feature_flag
    }
  end

  test "validate: wrong number of arguments results in arg count errors" do
    assert @command.validate([], %{}) == {:validation_failure, :not_enough_args}
    assert @command.validate(["ff_from_enable_ff_testsuite", "whoops"], %{}) == {:validation_failure, :too_many_args}
  end

  test "validate: passing an empty string for feature_flag name is an arg error", context do
    assert @command.validate([""], context[:opts]) == {:validation_failure, {:bad_argument, "feature_flag cannot be empty string."}}
  end

  test "run: passing a valid feature_flag name to a running RabbitMQ node succeeds", context do
    assert @command.run([Atom.to_string(context[:feature_flag])], context[:opts]) == :ok
    assert list_feature_flags(:enabled) |> Map.has_key?(context[:feature_flag])
  end

  test "run: attempt to use an unreachable node returns a nodedown" do
    opts = %{node: :jake@thedog, timeout: 3000}
    assert @command.run(["na"], opts) == {:badrpc, :nodedown}
  end

  test "run: enabling the same feature flag twice is idempotent", context do
    enable_feature_flag context[:feature_flag]
    assert @command.run([Atom.to_string(context[:feature_flag])], context[:opts]) == :ok
    assert list_feature_flags(:enabled) |> Map.has_key?(context[:feature_flag])
  end

  test "banner", context do
    assert @command.banner([context[:feature_flag]], context[:opts])
      =~ ~r/Enabling feature flag \"#{context[:feature_flag]}\" \.\.\./
  end
end
