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


defmodule DeleteUserCommandTest do
  use ExUnit.Case, async: false
  import ExUnit.CaptureIO
  import TestHelper

  setup_all do
    :net_kernel.start([:rabbitmqctl, :shortnames])
    on_exit([], fn -> :net_kernel.stop() end)
    :ok
  end

  setup context do
    :net_kernel.connect_node(context[:target])
    add_user(context[:user], "password")

    on_exit(context, fn ->
      delete_user(context[:user])
      :erlang.disconnect_node(context[:target])
    end)
    {:ok, opts: %{node: context[:target]}}
  end

	@tag target: get_rabbit_hostname, user: "username"
	test "The wrong number of arguments prints usage" do
		assert capture_io(fn -> DeleteUserCommand.delete_user([], %{}) end) =~ ~r/Usage:\n/
		assert capture_io(fn -> DeleteUserCommand.delete_user(["too", "many"], %{}) end) =~ ~r/Usage:\n/
	end

	@tag target: get_rabbit_hostname, user: "username"
	test "A valid username returns ok", context do
		assert DeleteUserCommand.delete_user([context[:user]], context[:opts]) == :ok
	end

	@tag target: :jake@thedog, user: "username"
	test "An invalid Rabbit node returns a bad rpc message", context do
		assert DeleteUserCommand.delete_user([context[:user]], context[:opts]) == {:badrpc, :nodedown}
	end

	@tag target: get_rabbit_hostname, user: "username"
	test "An invalid username returns an error", context do
		assert DeleteUserCommand.delete_user(["no_one"], context[:opts]) == {:error, {:no_such_user, "no_one"}}
	end
end
