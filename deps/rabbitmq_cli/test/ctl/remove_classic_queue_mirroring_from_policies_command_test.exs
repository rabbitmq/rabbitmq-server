## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2016-2020 VMware, Inc. or its affiliates.  All rights reserved.

defmodule RemoveClassicQueueMirroringFromPoliciesCommandTest do
  use ExUnit.Case, async: false
  import TestHelper

  @command RabbitMQ.CLI.Ctl.Commands.RemoveClassicQueueMirroringFromPoliciesCommand

  @vhost "/"

  setup_all do
    RabbitMQ.CLI.Core.Distribution.start()

    start_rabbitmq_app()

    on_exit([], fn ->
      start_rabbitmq_app()
    end)

    :ok
  end

  setup do
    {:ok,
     opts: %{
       node: get_rabbit_hostname(),
       vhost: @vhost
     }}
  end

  test "validate: specifying no arguments succeeds", context do
    assert @command.validate([], context[:opts]) == :ok
  end

  test "validate: specifying a position argument is reported as an error", context do
    assert @command.validate(["q1"], context[:opts]) ==
             {:validation_failure, :too_many_args}
  end

  test "validate: specifying three queue names is reported as an error", context do
    assert @command.validate(["q1", "q2", "q3"], context[:opts]) ==
             {:validation_failure, :too_many_args}
  end

  test "run: request to a non-existent RabbitMQ node returns a nodedown" do
    opts = %{node: :jake@thedog, vhost: @vhost, timeout: 200}
    assert match?({:badrpc, _}, @command.run([], opts))
  end

  test "banner", context do
    s = @command.banner([], context[:opts])

    assert s =~ ~r/Will remove/
  end
end
