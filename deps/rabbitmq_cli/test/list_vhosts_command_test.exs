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
## Copyright (c) 2007-2016 Pivotal Software, Inc.  All rights reserved.


defmodule ListVhostsCommandTest do
  use ExUnit.Case, async: false
  import ExUnit.CaptureIO
  import TestHelper

  @vhost1 "test1"
  @vhost2 "test2"
  @root   "/"

  setup_all do
    :net_kernel.start([:rabbitmqctl, :shortnames])
    :net_kernel.connect_node(get_rabbit_hostname)

    add_vhost @vhost1
    add_vhost @vhost2

    on_exit([], fn ->
      delete_vhost @vhost1
      delete_vhost @vhost2
      :erlang.disconnect_node(get_rabbit_hostname)
      :net_kernel.stop()
    end)

    name_result = [
      [{:name, @vhost1}],
      [{:name, @vhost2}],
      [{:name, @root}]
    ]

    tracing_result = [
      [{:tracing, false}],
      [{:tracing, false}],
      [{:tracing, false}]
    ]

    full_result = [
      [{:name, @vhost1}, {:tracing, false}],
      [{:name, @vhost2}, {:tracing, false}],
      [{:name, @root}, {:tracing, false}]
    ]

    transposed_result = [
      [{:tracing, false}, {:name, @vhost1}],
      [{:tracing, false}, {:name, @vhost2}],
      [{:tracing, false}, {:name, @root}]
    ]

    {
      :ok,
      name_result: name_result,
      tracing_result: tracing_result,
      full_result: full_result,
      transposed_result: transposed_result
    }
  end

  setup context do
    {
      :ok,
      opts: %{node: get_rabbit_hostname, timeout: context[:test_timeout]}
    }
  end

  test "on a bad RabbitMQ node, return a badrpc" do
    target = :jake@thedog
    opts = %{node: :jake@thedog, timeout: :infinity}
    :net_kernel.connect_node(target)
    capture_io(fn ->
      assert ListVhostsCommand.run([], opts) == {:badrpc, :nodedown}
    end)
  end

  @tag test_timeout: :infinity
  test "with no command, print just the names", context do

    # checks to ensure that all expected vhosts are in the results
    capture_io(fn ->
      matches_found = ListVhostsCommand.run([], context[:opts])

      assert Enum.all?(context[:name_result], fn(vhost) ->
        Enum.find(matches_found, fn(found) -> found == vhost end)
      end)
    end)
  end

  @tag test_timeout: :infinity
  test "with the name tag, print just the names", context do
    # checks to ensure that all expected vhosts are in the results
    capture_io(fn ->
      matches_found = ListVhostsCommand.run(["name"], context[:opts])
      assert Enum.all?(context[:name_result], fn(vhost) ->
        Enum.find(matches_found, fn(found) -> found == vhost end)
      end)
    end)
  end

  @tag test_timeout: :infinity
  test "with the tracing tag, print just say if tracing is on", context do
    # checks to ensure that all expected vhosts are in the results
    capture_io(fn ->
      matches_found = ListVhostsCommand.run(["tracing"], context[:opts])
      assert Enum.all?(context[:tracing_result], fn(vhost) ->
        Enum.find(matches_found, fn(found) -> found == vhost end)
      end)
    end)
  end

  @tag test_timeout: :infinity
  test "return bad_info_key on a single bad arg", context do
    capture_io(fn ->
      assert ListVhostsCommand.run(["quack"], context[:opts]) ==
        {:error, {:bad_info_key, ["quack"]}}
    end)
  end

  @tag test_timeout: :infinity
  test "multiple bad args return a list of bad info key values", context do
    capture_io(fn ->
      assert ListVhostsCommand.run(["quack", "oink"], context[:opts]) ==
        {:error, {:bad_info_key, ["quack", "oink"]}}
    end)
  end

  @tag test_timeout: :infinity
  test "return bad_info_key on mix of good and bad args", context do
    capture_io(fn ->
      assert ListVhostsCommand.run(["quack", "tracing"], context[:opts]) ==
        {:error, {:bad_info_key, ["quack"]}}
      assert ListVhostsCommand.run(["name", "oink"], context[:opts]) ==
        {:error, {:bad_info_key, ["oink"]}}
      assert ListVhostsCommand.run(["name", "oink", "tracing"], context[:opts]) ==
        {:error, {:bad_info_key, ["oink"]}}
    end)
  end

  @tag test_timeout: :infinity
  test "with name and tracing keys, print both", context do
    # checks to ensure that all expected vhosts are in the results
    capture_io(fn ->
      matches_found = ListVhostsCommand.run(["name", "tracing"], context[:opts])
      assert Enum.all?(context[:full_result], fn(vhost) ->
        Enum.find(matches_found, fn(found) -> found == vhost end)
      end)
    end)

    # checks to ensure that all expected vhosts are in the results
    capture_io(fn ->
      matches_found = ListVhostsCommand.run(["tracing", "name"], context[:opts])
      assert Enum.all?(context[:transposed_result], fn(vhost) ->
        Enum.find(matches_found, fn(found) -> found == vhost end)
      end)
    end)
  end

  @tag test_timeout: :infinity
  test "duplicate args do not produce duplicate entries", context do
    # checks to ensure that all expected vhosts are in the results
    capture_io(fn ->
      matches_found = ListVhostsCommand.run(["name", "name"], context[:opts])
      assert Enum.all?(context[:name_result], fn(vhost) ->
        Enum.find(matches_found, fn(found) -> found == vhost end)
      end)
    end)
  end

  @tag test_timeout: 30
  test "sufficiently long timeouts don't interfere with results", context do
    # checks to ensure that all expected vhosts are in the results
    capture_io(fn ->
      matches_found = ListVhostsCommand.run(["name", "tracing"], context[:opts])
      assert Enum.all?(context[:full_result], fn(vhost) ->
        Enum.find(matches_found, fn(found) -> found == vhost end)
      end)
    end)
  end

  @tag test_timeout: 0, username: "guest"
  test "timeout causes command to return a bad RPC", context do
    capture_io(fn ->
      assert ListVhostsCommand.run(["name", "tracing"], context[:opts]) == 
        {:badrpc, :timeout}
    end)
  end

  @tag test_timeout: :infinity
  test "print info message by default", context do
    assert capture_io(fn ->
      ListVhostsCommand.run([], context[:opts])
    end) =~ ~r/Listing vhosts \.\.\./
  end

  @tag test_timeout: :infinity
  test "--quiet flag suppresses info message", context do
    opts = Map.merge(context[:opts], %{quiet: true})
    refute capture_io(fn ->
      ListVhostsCommand.run([], opts)
    end) =~ ~r/Listing vhosts \.\.\./
  end
end
