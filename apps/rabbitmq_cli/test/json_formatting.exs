## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2020 VMware, Inc. or its affiliates.  All rights reserved.


defmodule JSONFormattingTest do
  use ExUnit.Case, async: false
  import ExUnit.CaptureIO
  import RabbitMQ.CLI.Core.ExitCodes
  import TestHelper

  setup_all do
    RabbitMQ.CLI.Core.Distribution.start()

    set_scope(:all)

    :ok
  end

  test "JSON output of status" do
    set_scope(:ctl)

    node = to_string(get_rabbit_hostname())
    command = ["status", "-n", node, "--formatter=json"]
    output = capture_io(:stdio, fn ->
      error_check(command, exit_ok())
    end)
    {:ok, doc} = JSON.decode(output)

    assert Map.has_key?(doc, "memory")
    assert Map.has_key?(doc, "file_descriptors")
    assert Map.has_key?(doc, "listeners")
    assert Map.has_key?(doc, "processes")
    assert Map.has_key?(doc, "os")
    assert Map.has_key?(doc, "pid")
    assert Map.has_key?(doc, "rabbitmq_version")

    assert doc["alarms"] == []
  end

  test "JSON output of cluster_status" do
    set_scope(:ctl)

    node = to_string(get_rabbit_hostname())
    command = ["cluster_status", "-n", node, "--formatter=json"]
    output = capture_io(:stdio, fn ->
      error_check(command, exit_ok())
    end)
    {:ok, doc} = JSON.decode(output)

    assert Enum.member?(doc["disk_nodes"], node)
    assert Map.has_key?(doc["listeners"], node)
    assert Map.has_key?(doc["versions"], node)
    assert doc["alarms"] == []
    assert doc["partitions"] == %{}
  end
end
