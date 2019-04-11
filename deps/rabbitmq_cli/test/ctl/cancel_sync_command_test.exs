## The contents of this file are subject to the Mozilla Public License
## Version 1.1 (the "License"); you may not use this file except in
## compliance with the License. You may obtain a copy of the License
## at https://www.mozilla.org/MPL/
##
## Software distributed under the License is distributed on an "AS IS"
## basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
## the License for the specific language governing rights and
## limitations under the License.
##
## The Original Code is RabbitMQ.
##
## The Initial Developer of the Original Code is Pivotal Software, Inc.
## Copyright (c) 2016-2017 Pivotal Software, Inc.  All rights reserved.

defmodule CancelSyncQueueCommandTest do
  use ExUnit.Case, async: false
  import TestHelper

  @command RabbitMQ.CLI.Ctl.Commands.CancelSyncQueueCommand

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
    {:ok, opts: %{
      node: get_rabbit_hostname(),
      vhost: @vhost
    }}
  end

  test "validate: specifying no queue name is reported as an error", context do
    assert @command.validate([], context[:opts]) ==
      {:validation_failure, :not_enough_args}
  end

  test "validate: specifying two queue names is reported as an error", context do
    assert @command.validate(["q1", "q2"], context[:opts]) ==
      {:validation_failure, :too_many_args}
  end

  test "validate: specifying three queue names is reported as an error", context do
    assert @command.validate(["q1", "q2", "q3"], context[:opts]) ==
      {:validation_failure, :too_many_args}
  end

  test "validate: specifying one queue name succeeds", context do
    assert @command.validate(["q1"], context[:opts]) == :ok
  end

  test "run: request to a non-existent RabbitMQ node returns a nodedown" do
    opts = %{node: :jake@thedog, vhost: @vhost, timeout: 200}
    assert match?({:badrpc, _}, @command.run(["q1"], opts))
  end

  test "banner", context do
    s = @command.banner(["q1"], context[:opts])

    assert s =~ ~r/Stopping synchronising queue/
    assert s =~ ~r/q1/
  end
end
