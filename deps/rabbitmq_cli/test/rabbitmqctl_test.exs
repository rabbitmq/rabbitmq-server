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


defmodule RabbitMQCtlTest do
  use ExUnit.Case, async: false
  import ExUnit.CaptureIO

  setup_all do
    :net_kernel.start([:rabbitmqctl, :shortnames])
    on_exit([], fn -> :net_kernel.stop() end)
    :ok
  end


## ------------------------ Error Messages ------------------------------------

  test "print error message on a bad connection" do
    command = ["status", "-n", "sandwich@pastrami"]
    assert capture_io(fn -> RabbitMQCtl.main(command) end) =~ ~r/unable to connect to node 'sandwich@pastrami'\: nodedown/
  end

  test "print timeout message when an RPC call times out" do
    command = ["status", "-n", "sandwich@pastrami"]
    assert capture_io(fn -> RabbitMQCtl.main(command) end) =~ ~r/unable to connect to node 'sandwich@pastrami'\: nodedown/
  end

## ------------------------ Malformed Commands --------------------------------

  test "Empty command shows usage message" do
    assert capture_io(fn -> RabbitMQCtl.main([]) end) =~ ~r/Usage\:/
  end

  test "Empty command with options shows usage message" do
    assert capture_io(fn -> RabbitMQCtl.main(["-n", "sandwich@pastrami"]) end) =~ ~r/Usage\:/
  end

  test "Unimplemented command shows usage message" do
    assert capture_io(fn -> RabbitMQCtl.main(["not_real"]) end) =~ ~r/Usage\:/
  end

## ------------------------- Default Flags ------------------------------------

  test "an empty node option is filled with the default rabbit node" do
    assert RabbitMQCtl.autofill_defaults(%{})[:node] ==
      TestHelper.get_rabbit_hostname
  end

  test "a non-empty node option is not overwritten" do
    assert RabbitMQCtl.autofill_defaults(%{node: :jake@thedog})[:node] ==
      :jake@thedog
  end

  test "an empty timeout option is set to infinity" do
    assert RabbitMQCtl.autofill_defaults(%{})[:timeout] == :infinity
  end

  test "a non-empty timeout option is not overridden" do
    assert RabbitMQCtl.autofill_defaults(%{timeout: 60})[:timeout] == 60
  end
end
