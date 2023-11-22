## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2023 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries.  All rights reserved.

defmodule CheckIfClusterHasClassicQueueMirroringPolicyCommandTest do
  use ExUnit.Case, async: false
  import TestHelper

  @command RabbitMQ.CLI.Diagnostics.Commands.CheckIfClusterHasClassicQueueMirroringPolicyCommand
  @policy_name "cmq-policy-8373"
  @policy_value "{\"ha-mode\":\"all\"}"

  setup_all do
    RabbitMQ.CLI.Core.Distribution.start()

    start_rabbitmq_app()

    on_exit([], fn ->
      start_rabbitmq_app()
      clear_policy("/", @policy_name)
    end)

    :ok
  end

  setup context do
    {:ok,
     opts: %{
       node: get_rabbit_hostname(),
       timeout: context[:test_timeout] || 30000
     }}
  end

  test "merge_defaults: nothing to do" do
    assert @command.merge_defaults([], %{}) == {[], %{}}
  end

  test "validate: treats positional arguments as a failure" do
    assert @command.validate(["extra-arg"], %{}) == {:validation_failure, :too_many_args}
  end

  test "validate: treats empty positional arguments and default switches as a success" do
    assert @command.validate([], %{}) == :ok
  end

  @tag test_timeout: 3000
  test "run: targeting an unreachable node throws a badrpc", context do
    assert match?(
             {:badrpc, _},
             @command.run([], Map.merge(context[:opts], %{node: :jake@thedog}))
           )
  end

  test "run: when there are no policies that enable CMQ mirroring, reports success", context do
    clear_policy("/", @policy_name)
    assert @command.run([], context[:opts])
  end

  test "output: when the result is true, returns successfully", context do
    assert match?({:ok, _}, @command.output(true, context[:opts]))
  end

  # this is a check command
  test "output: when the result is false, returns an error", context do
    assert match?({:error, _}, @command.output(false, context[:opts]))
  end
end
