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
## The Initial Developer of the Original Code is GoPivotal, Inc.
## Copyright (c) 2007-2020 Pivotal Software, Inc.  All rights reserved.


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
