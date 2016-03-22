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


defmodule ChangePasswordCommandTest do
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
    on_exit(context, fn -> delete_user(context[:user]) end)
    {:ok, opts: %{node: get_rabbit_hostname}}
  end

  test "invalid arguments print return bad_argument" do
    assert capture_io(fn ->
      ChangePasswordCommand.change_password([], %{})
    end) =~ ~r/Usage:\n/

    capture_io(fn->
      assert ChangePasswordCommand.change_password([], %{}) ==
        {:bad_argument, ["<missing>", "<missing>"]}
    end)

    assert capture_io(fn ->
      ChangePasswordCommand.change_password(["user"], %{})
    end) =~ ~r/Usage:\n/

    capture_io(fn->
      assert ChangePasswordCommand.change_password(["user"], %{}) ==
        {:bad_argument, ["user", "<missing>"]}
    end)

    assert capture_io(fn ->
      ChangePasswordCommand.change_password(["user", "password", "extra"], %{})
    end) =~ ~r/Usage:\n/

    capture_io(fn->
      assert ChangePasswordCommand.change_password(["user", "password", "extra"], %{}) ==
        {:bad_argument, ["extra"]}
    end)
  end

  @tag user: @user, password: "new_password"
  test "a valid username and new password return ok", context do
    assert ChangePasswordCommand.change_password([context[:user], context[:password]], context[:opts]) == :ok

    assert {:ok, _} = authenticate_user(context[:user], context[:password])
  end

  test "An invalid rabbitmq node throws a badrpc" do
    target = :jake@thedog
    :net_kernel.connect_node(target)
    opts = %{node: target}

    assert ChangePasswordCommand.change_password(["user", "password"], opts) == {:badrpc, :nodedown}
  end

  @tag user: @user, password: @password
  test "changing password to the same thing is ok", context do
    assert ChangePasswordCommand.change_password([context[:user], context[:password]], context[:opts]) == :ok

    assert {:ok, _} = authenticate_user(context[:user], context[:password])
  end

  @tag user: "interloper", password: "new_password"
  test "an invalid user returns an error", context do
    assert ChangePasswordCommand.change_password([context[:user], context[:password]], context[:opts]) == {:error, {:no_such_user, "interloper"}}
  end
end
