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


defmodule ClearPasswordCommandTest do
  use ExUnit.Case, async: false
  import ExUnit.CaptureIO
  import TestHelper

  @user     "user1"
  @password "password"

  setup_all do
    :net_kernel.start([:rabbitmqctl, :shortnames])
    :net_kernel.connect_node(get_rabbit_hostname)

    on_exit([], fn ->
      :erlang.disconnect_node(get_rabbit_hostname)
      :net_kernel.stop()
    end)

    :ok
  end

  setup context do
    add_user(@user, @password)
    on_exit(context, fn -> delete_user(@user) end)
    {:ok, opts: %{node: get_rabbit_hostname}}
  end

  test "invalid arguments print return bad_argument" do
    assert capture_io(fn ->
      ClearPasswordCommand.clear_password([], %{})
    end) =~ ~r/Usage:\n/

    capture_io(fn->
      assert ClearPasswordCommand.clear_password([], %{}) ==
        {:bad_argument, ["<missing>"]}
    end)

    assert capture_io(fn ->
      ClearPasswordCommand.clear_password(["username", "extra"], %{})
    end) =~ ~r/Usage:\n/

    capture_io(fn->
      assert ClearPasswordCommand.clear_password(["username", "extra"], %{}) ==
        {:bad_argument, ["extra"]}
    end)
  end

  @tag user: @user, password: @password
  test "a valid username clears the password and returns okay", context do
    assert ClearPasswordCommand.clear_password([context[:user]], context[:opts]) == :ok

     assert {:refused, _, _, _} = authenticate_user(context[:user], context[:password])
  end

  test "An invalid rabbitmq node throws a badrpc" do
    target = :jake@thedog
    :net_kernel.connect_node(target)
    opts = %{node: target}

    assert ClearPasswordCommand.clear_password(["user"], opts) == {:badrpc, :nodedown}
  end

  @tag user: "interloper"
  test "An invalid username returns a no-such-user error message", context do
    assert ClearPasswordCommand.clear_password([context[:user]], context[:opts]) == {:error, {:no_such_user, "interloper"}}
  end
end
