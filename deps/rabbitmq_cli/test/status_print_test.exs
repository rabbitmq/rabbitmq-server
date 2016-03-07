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


# defmodule StatusPrintTest do
  # use ExUnit.Case, async: false
  # import TestHelper
  # import ExUnit.CaptureIO

  # setup_all do
    # :net_kernel.start([:rabbitmqctl, :shortnames])
    # on_exit([], fn -> :net_kernel.stop() end)
    # :ok
  # end


  # setup context do
    # target = get_rabbit_hostname
    # :net_kernel.connect_node(target)
    # on_exit(context, fn -> :erlang.disconnect_node(target) end)
    # {:ok, result: StatusCommand.status([], [])}
  # end

  # test "a non-list result does not print anything" do
    # assert capture_io(fn -> print({:bad_result, "oh no"}) end) == ""
  # end

  # test "status shows PID", context do
    # assert capture_io(fn -> print(context[:result])  end) =~ ~r/PID\: \d+/
  # end

  # test "status shows Erlang version", context do
    # assert capture_io(fn -> print(context[:result])  end) =~ ~r/OTP version: \d+/
    # assert capture_io(fn -> print(context[:result])  end) =~ ~r/Erlang RTS version: \d+\.\d+\.\d+/
  # end

  # test "status shows info about the operating system", context do
    # os_name = :os.type |> elem(1) |> Atom.to_string |> Mix.Utils.camelize
    # os_regex = ~r/OS: #{os_name}\n/
    # assert Regex.match?(os_regex, capture_io(fn -> print(context[:result]) end))
  # end

  # test "status shows running apps", context do
    # assert capture_io(fn -> print(context[:result]) end) =~ ~r/Applications currently running\:\n/
    # assert capture_io(fn -> print(context[:result]) end) =~ ~r/-------\n/
    # assert capture_io(fn -> print(context[:result]) end) =~ ~r/\[rabbit\]\s*| RabbitMQ\s*| \d+.\d+.\d+\n/
  # end

 # test "status shows memory usage", context do
   # assert capture_io(fn -> print(context[:result]) end) =~ ~r/Memory usage\:\n/
   # assert capture_io(fn -> print(context[:result]) end) =~ ~r/------\n/
   # assert capture_io(fn -> print(context[:result]) end) =~ ~r/Total\s*| \d+\n/
 # end

 # test "status shows memory high water mark", context do
   # assert capture_io(fn -> print(context[:result]) end) =~ ~r/VM Memory High Water Mark: \d+\.\d+\n/
 # end

 # test "status shows memory limit", context do
   # assert capture_io(fn -> print(context[:result]) end) =~ ~r/VM Memory Limit: \d+\n/
 # end

 # test "status shows alarms (none)", context do
   # assert capture_io(fn -> print(context[:result]) end) =~ ~r/Resource Alarms: None\n/
 # end

 # test "status shows table of listeners", context do
   # assert capture_io(fn -> print(context[:result]) end) =~ ~r/Listeners:\n/
   # assert capture_io(fn -> print(context[:result]) end) =~ ~r/------\n/
   # assert capture_io(fn -> print(context[:result]) end) =~ ~r/amqp\s+| 5672\s+| ::\n/
 # end

 # test "status shows disk free limit", context do
   # assert capture_io(fn -> print(context[:result]) end) =~ ~r/Disk Free Limit: \d+\n/
 # end

 # test "status shows disk free total", context do
   # assert capture_io(fn -> print(context[:result]) end) =~ ~r/Disk Free: \d+\n/
 # end

  # test "status shows file descriptor data", context do
   # assert capture_io(fn -> print(context[:result]) end) =~ ~r/File Descriptor Stats:\n/
   # assert capture_io(fn -> print(context[:result]) end) =~ ~r/------\n/
   # assert capture_io(fn -> print(context[:result]) end) =~ ~r/total_limit\s+| \d+\n/
  # end

  # test "status shows process data", context do
   # assert capture_io(fn -> print(context[:result]) end) =~ ~r/RabbitMQ Process Stats:\n/
   # assert capture_io(fn -> print(context[:result]) end) =~ ~r/------\n/
   # assert capture_io(fn -> print(context[:result]) end) =~ ~r/limit\s+| \d+\n/
  # end


  # test "status shows run queue content", context do
    # assert capture_io(fn -> print(context[:result]) end) =~ ~r/Run Queue: \d+\n/
  # end

  # test "status shows uptime", context do
    # assert capture_io(fn -> print(context[:result]) end) =~ ~r/Broker Uptime: \d+\n/
  # end

  # test "status shows tick time", context do
    # assert capture_io(fn -> print(context[:result]) end) =~ ~r/Network Tick Time: \d+\n/
  # end

  # # Helper method for printing tests
  # defp print(result) do
    # StatusPrint.print_status(result)
  # end
# end

