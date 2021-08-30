## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2020 VMware, Inc. or its affiliates.  All rights reserved.

defmodule CheckLocalAlarmsCommandTest do
  use ExUnit.Case, async: false
  import TestHelper
  import RabbitMQ.CLI.Core.Alarms, only: [alarm_types: 1]

  @command RabbitMQ.CLI.Diagnostics.Commands.CheckLocalAlarmsCommand

  setup_all do
    RabbitMQ.CLI.Core.Distribution.start()

    start_rabbitmq_app()

    on_exit([], fn ->
      start_rabbitmq_app()
    end)

    :ok
  end

  setup context do
    {:ok, opts: %{
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
    assert match?({:badrpc, _}, @command.run([], Map.merge(context[:opts], %{node: :jake@thedog})))
  end

  test "run: when target node has no alarms in effect, returns an empty list", context do
    assert [] == status()[:alarms]

    assert @command.run([], context[:opts]) == []
  end

  test "run: when target node has a local alarm in effect, returns it", context do
    old_watermark = status()[:vm_memory_high_watermark]
    on_exit(fn() ->
      set_vm_memory_high_watermark(old_watermark)
    end)
    # 2000 bytes will trigger an alarm
    set_vm_memory_high_watermark({:absolute, 2000})

    assert [:memory] == alarm_types(status()[:alarms])
    assert length(@command.run([], context[:opts])) == 1

    set_vm_memory_high_watermark(old_watermark)
  end

  test "output: when target node has no local alarms in effect, returns a success", context do
    assert [] == status()[:alarms]

    assert match?({:ok, _}, @command.output([], context[:opts]))
  end

  # note: it's run/2 that filters out non-local alarms
  test "output: when target node has a local alarm in effect, returns a failure", context do
    for input <- [
          [
            :file_descriptor_limit
          ],
          [
            :file_descriptor_limit,
            {{:resource_limit, :disk,   get_rabbit_hostname()}, []},
            {{:resource_limit, :memory, get_rabbit_hostname()}, []}
          ]
        ] do
        assert match?({:error, _}, @command.output(input, context[:opts]))
    end
  end

  # note: it's run/2 that filters out non-local alarms
  test "output: when target node has an alarm in effect and --silent is passed, returns a silent failure", _context do
    for input <- [
          [
            :file_descriptor_limit
          ],
          [
            :file_descriptor_limit,
            {{:resource_limit, :disk, :hare@warp10}, []}
          ],
          [
            :file_descriptor_limit,
            {{:resource_limit, :disk,   get_rabbit_hostname()}, []},
            {{:resource_limit, :memory, get_rabbit_hostname()}, []}
          ]
        ] do
        assert {:error, :check_failed} == @command.output(input, %{silent: true})
    end
  end
end
