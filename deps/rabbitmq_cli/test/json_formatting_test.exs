## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries.  All rights reserved.

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

    output =
      capture_io(:stdio, fn ->
        error_check(command, exit_ok())
      end)

    {:ok, doc} = JSON.decode(output)

    assert Map.has_key?(doc, "memory")
    assert Map.has_key?(doc, "listeners")
    assert Map.has_key?(doc, "processes")
    assert Map.has_key?(doc, "os")
    assert Map.has_key?(doc, "pid")
    assert Map.has_key?(doc, "rabbitmq_version")
    assert Map.has_key?(doc, "active_plugins")
    assert Map.has_key?(doc, "config_files")
    assert Map.has_key?(doc, "log_files")

    active_plugins = doc["active_plugins"]
    assert is_list(active_plugins)
    assert Enum.all?(active_plugins, &is_binary/1)

    config_files = doc["config_files"]
    assert is_list(config_files)
    assert Enum.all?(config_files, &is_binary/1)

    log_files = doc["log_files"]
    assert is_list(log_files)
    assert Enum.all?(log_files, &is_binary/1)

    assert doc["alarms"] == []
  end

  test "JSON output of cluster_status" do
    set_scope(:ctl)

    node = to_string(get_rabbit_hostname())
    command = ["cluster_status", "-n", node, "--formatter=json"]

    output =
      capture_io(:stdio, fn ->
        error_check(command, exit_ok())
      end)

    {:ok, doc} = JSON.decode(output)

    assert Map.has_key?(doc, "running_nodes")
    running_nodes = doc["running_nodes"]
    assert is_list(running_nodes)
    assert Enum.all?(running_nodes, &is_binary/1)

    assert Enum.member?(doc["disk_nodes"], node)
    assert Map.has_key?(doc["listeners"], node)
    assert Map.has_key?(doc["versions"], node)
    assert Map.has_key?(doc["versions"], node)
    assert doc["alarms"] == []
    assert doc["partitions"] == %{}
  end

  test "JSON output of environment" do
    set_scope(:ctl)

    node = to_string(get_rabbit_hostname())
    command = ["environment", "-n", node, "--formatter=json"]

    output =
      capture_io(:stdio, fn ->
        error_check(command, exit_ok())
      end)

    {:ok, doc} = JSON.decode(output)
    assert Map.has_key?(doc, "rabbit")
    rabbit = doc["rabbit"]
    assert Map.has_key?(rabbit, "data_dir")
    data_dir = rabbit["data_dir"]
    assert is_binary(data_dir)

    assert Map.has_key?(rabbit, "tcp_listeners")
    tcp_listeners = rabbit["tcp_listeners"]
    assert is_list(tcp_listeners)
  end
end
