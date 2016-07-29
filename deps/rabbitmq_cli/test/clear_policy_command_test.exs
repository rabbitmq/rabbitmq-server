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


defmodule ClearPolicyCommandTest do
  use ExUnit.Case, async: false
  import TestHelper

  @command RabbitMQ.CLI.Ctl.Commands.ClearPolicyCommand
  @vhost "test1"
  @user "guest"
  @root   "/"
  @key "federate"
  @pattern "^fed\."
  @value "{\"federation-upstream-set\":\"all\"}"
  @apply_to "all"
  @priority 0

  setup_all do
    RabbitMQ.CLI.Distribution.start()
    :net_kernel.connect_node(get_rabbit_hostname)

    add_vhost @vhost

    on_exit([], fn ->
      delete_vhost @vhost
      :erlang.disconnect_node(get_rabbit_hostname)
      :net_kernel.stop()
    end)

    :ok
  end

  setup context do
    on_exit(context, fn ->
      clear_policy context[:vhost], context[:key]
    end)

    {
      :ok,
      opts: %{
        node: get_rabbit_hostname
      }
    }
  end

  test "merge_defaults: adds default vhost if missing" do
    assert @command.merge_defaults([], %{}) == {[], %{vhost: "/"}}
  end

  test "merge_defaults: does not change defined vhost" do
    assert @command.merge_defaults([], %{vhost: "test_vhost"}) == {[], %{vhost: "test_vhost"}}
  end

  test "validate: argument validation" do
    assert @command.validate(["one"], %{}) == :ok
    assert @command.validate([], %{}) == {:validation_failure, :not_enough_args}
    assert @command.validate(["too", "many"], %{}) == {:validation_failure, :too_many_args}
    assert @command.validate(["this", "is", "many"], %{}) == {:validation_failure, :too_many_args}
  end

  @tag pattern: @pattern, key: @key, vhost: @vhost
  test "run: returns error, if policy does not exist", context do
    vhost_opts = Map.merge(context[:opts], %{vhost: context[:vhost]})

    assert @command.run(
      [context[:key]],
      vhost_opts
    ) == {:error_string, 'Parameter does not exist'}
  end

  test "run: An invalid rabbitmq node throws a badrpc" do
    target = :jake@thedog
    :net_kernel.connect_node(target)
    opts = %{node: target, vhost: "/"}
    assert @command.run([@key], opts) == {:badrpc, :nodedown}
  end


  @tag pattern: @pattern, key: @key, vhost: @vhost
  test "run: returns ok and clears parameter, if it exists", context do
    vhost_opts = Map.merge(context[:opts], %{vhost: context[:vhost]})

    set_policy(context[:vhost], context[:key], context[:pattern], @value)

    assert @command.run(
      [context[:key]],
      vhost_opts
    ) == :ok

    assert_policy_empty(context)
  end

  @tag pattern: @pattern, key: @key, value: @value, vhost: "bad-vhost"
  test "run: an invalid vhost returns a 'parameter does not exist' error", context do
    vhost_opts = Map.merge(context[:opts], %{vhost: context[:vhost]})

    assert @command.run(
      [context[:key]],
      vhost_opts
    ) == {:error_string, 'Parameter does not exist'}
  end

  @tag key: @key, pattern: @pattern, value: @value, vhost: @vhost
  test "banner", context do
    vhost_opts = Map.merge(context[:opts], %{vhost: context[:vhost]})
    set_policy(context[:vhost], context[:key], context[:pattern], @value)

    s = @command.banner(
      [context[:key]],
      vhost_opts
    )

    assert s =~ ~r/Clearing policy/
    assert s =~ ~r/"#{context[:key]}"/
  end

  defp assert_policy_empty(context) do
    policy = context[:vhost]
                |> list_policies
                |> Enum.filter(fn(param) ->
                    param[:pattern] == context[:pattern] and
                    param[:key] == context[:key]
                    end)
    assert policy === []
  end
end
