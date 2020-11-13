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
## Copyright (c) 2007-2019 Pivotal Software, Inc.  All rights reserved.


defmodule ClearPolicyCommandTest do
  use ExUnit.Case, async: false
  import TestHelper

  @command RabbitMQ.CLI.Ctl.Commands.ClearPolicyCommand
  @vhost "test1"
  @key "federate"
  @pattern "^fed\."
  @value "{\"federation-upstream-set\":\"all\"}"

  setup_all do
    RabbitMQ.CLI.Core.Distribution.start()


    add_vhost @vhost

    enable_federation_plugin()

    on_exit([], fn ->
      delete_vhost @vhost


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
        node: get_rabbit_hostname()
      }
    }
  end

  test "merge_defaults: adds default vhost if missing" do
    assert @command.merge_defaults([], %{}) == {[], %{vhost: "/"}}
  end

  test "merge_defaults: does not change defined vhost" do
    assert @command.merge_defaults([], %{vhost: "test_vhost"}) == {[], %{vhost: "test_vhost"}}
  end

  test "validate: providing too few arguments fails validation" do
    assert @command.validate([], %{}) == {:validation_failure, :not_enough_args}
  end

  test "validate: providing too many arguments fails validation" do
    assert @command.validate(["too", "many"], %{}) == {:validation_failure, :too_many_args}
    assert @command.validate(["this", "is", "many"], %{}) == {:validation_failure, :too_many_args}
  end

  test "validate: providing one argument and no options passes validation" do
    assert @command.validate(["a-policy"], %{}) == :ok
  end

  @tag pattern: @pattern, key: @key, vhost: @vhost
  test "run: if policy does not exist, returns an error", context do
    vhost_opts = Map.merge(context[:opts], %{vhost: context[:vhost]})

    assert @command.run(
      [context[:key]],
      vhost_opts
    ) == {:error_string, 'Parameter does not exist'}
  end

  test "run: an unreachable node throws a badrpc" do
    opts = %{node: :jake@thedog, vhost: "/", timeout: 200}
    assert match?({:badrpc, _}, @command.run([@key], opts))
  end


  @tag pattern: @pattern, key: @key, vhost: @vhost
  test "run: if policy exists, returns ok and removes it", context do
    vhost_opts = Map.merge(context[:opts], %{vhost: context[:vhost]})

    set_policy(context[:vhost], context[:key], context[:pattern], @value)

    assert @command.run(
      [context[:key]],
      vhost_opts
    ) == :ok

    assert_policy_does_not_exist(context)
  end

  @tag pattern: @pattern, key: @key, value: @value, vhost: "bad-vhost"
  test "run: a non-existent vhost returns an error", context do
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

  defp assert_policy_does_not_exist(context) do
    policy = context[:vhost]
                |> list_policies
                |> Enum.filter(fn(param) ->
                    param[:pattern] == context[:pattern] and
                    param[:key] == context[:key]
                    end)
    assert policy === []
  end
end
