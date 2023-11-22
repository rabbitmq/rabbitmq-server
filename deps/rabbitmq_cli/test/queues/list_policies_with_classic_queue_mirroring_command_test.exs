## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2023 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries.  All rights reserved.

defmodule RabbitMQ.CLI.Queues.Commands.ListPoliciesWithClassicQueueMirroringCommandTest do
  use ExUnit.Case, async: false
  import TestHelper

  @command RabbitMQ.CLI.Queues.Commands.ListPoliciesWithClassicQueueMirroringCommand

  setup_all do
    RabbitMQ.CLI.Core.Distribution.start()

    :ok
  end

  setup context do
    {:ok,
     opts: %{
       node: get_rabbit_hostname(),
       timeout: context[:test_timeout] || 30000
     }}
  end

  test "validate: treats no arguments as a success" do
    assert @command.validate([], %{}) == :ok
  end

  test "validate: accepts no positional arguments" do
    assert @command.validate(["arg"], %{}) == {:validation_failure, :too_many_args}
  end

  test "validate: when two or more arguments are provided, returns a failure" do
    assert @command.validate(["arg1", "arg2"], %{}) ==
             {:validation_failure, :too_many_args}

    assert @command.validate(["arg1", "arg2", "arg3"], %{}) ==
             {:validation_failure, :too_many_args}
  end

  @tag test_timeout: 3000
  test "run: targeting an unreachable node throws a badrpc" do
    assert match?(
             {:badrpc, _},
             @command.run(
               [],
               %{node: :jake@thedog, vhost: "/", timeout: 200}
             )
           )
  end
end
