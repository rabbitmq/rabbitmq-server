## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2020 VMware, Inc. or its affiliates.  All rights reserved.


defmodule ListOperatorPoliciesCommandTest do
  use ExUnit.Case, async: false
  import TestHelper

  @command RabbitMQ.CLI.Ctl.Commands.ListOperatorPoliciesCommand

  @vhost "test1"
  @root   "/"
  @key "message-expiry"
  @pattern "^queue\."
  @value "{\"message-ttl\":10}"
  @apply_to "all"
  @default_options %{vhost: "/", table_headers: true}

  setup_all do
    RabbitMQ.CLI.Core.Distribution.start()

    add_vhost @vhost

    on_exit(fn ->
      delete_vhost @vhost
    end)

    :ok
  end

  setup context do
    on_exit(fn ->
      clear_operator_policy context[:vhost], context[:key]
    end)
    {
      :ok,
      opts: %{
        node: get_rabbit_hostname(),
        timeout: (context[:timeout] || :infinity),
        vhost: context[:vhost],
        apply_to: @apply_to,
        priority: 0
      }
    }
  end

  test "merge_defaults: default vhost is '/'" do
    assert @command.merge_defaults([], %{}) == {[], @default_options}
    assert @command.merge_defaults([], %{vhost: "non_default"}) == {[], %{vhost: "non_default",
                                                                          table_headers: true}}
  end

  test "validate: providing too many arguments fails validation" do
    assert @command.validate(["many"], %{}) == {:validation_failure, :too_many_args}
    assert @command.validate(["too", "many"], %{}) == {:validation_failure, :too_many_args}
    assert @command.validate(["this", "is", "too", "many"], %{}) == {:validation_failure, :too_many_args}
  end

  @tag key: @key, pattern: @pattern, value: @value, vhost: @vhost
  test "run: a well-formed, host-specific command returns list of policies", context do
    vhost_opts = Map.merge(context[:opts], %{vhost: context[:vhost]})
    set_operator_policy(context[:vhost], context[:key], context[:pattern], @value)
    @command.run([], vhost_opts)
    |> assert_operator_policy_list(context)
  end

  test "run: an unreachable node throws a badrpc" do
    opts = %{node: :jake@thedog, vhost: @vhost, timeout: 200}

    assert match?({:badrpc, _}, @command.run([], opts))
  end

  @tag key: @key, pattern: @pattern, value: @value, vhost: @root
  test "run: a well-formed command with no vhost runs against the default one", context do

    set_operator_policy("/", context[:key], context[:pattern], @value)
    on_exit(fn ->
      clear_operator_policy("/", context[:key])
    end)

    @command.run([], context[:opts])
    |> assert_operator_policy_list(context)
  end

  @tag key: @key, pattern: @pattern, value: @value, vhost: @vhost
  test "run: providing a timeout of 0 returns a badrpc", context do
    set_operator_policy(context[:vhost], context[:key], context[:pattern], @value)
    assert @command.run([], Map.put(context[:opts], :timeout, 0)) == {:badrpc, :timeout}
  end

  @tag key: @key, pattern: @pattern, value: @value, vhost: "bad-vhost"
  test "run: providing a non-existent vhost returns an error", context do
    vhost_opts = Map.merge(context[:opts], %{vhost: context[:vhost]})

    assert @command.run(
      [],
      vhost_opts
    ) == {:error, {:no_such_vhost, context[:vhost]}}
  end

  @tag vhost: @vhost
  test "run: when multiple policies exist in the vhost, returns them all", context do
    policies = [
      %{vhost: @vhost, name: "some-policy", pattern: "foo", definition: "{\"message-ttl\":10}", 'apply-to': "all", priority: 0},
      %{vhost: @vhost, name: "other-policy", pattern: "bar", definition: "{\"expires\":20}", 'apply-to': "all", priority: 0}
    ]
    policies
    |> Enum.map(
        fn(%{name: name, pattern: pattern, definition: value}) ->
          set_operator_policy(context[:vhost], name, pattern, value)
          on_exit(fn ->
            clear_operator_policy(context[:vhost], name)
          end)
        end)

    pols = for policy <- @command.run([], context[:opts]), do: Map.new(policy)

    assert MapSet.new(pols) == MapSet.new(policies)
  end

  @tag key: @key, pattern: @pattern, value: @value, vhost: @vhost
  test "banner", context do
    vhost_opts = Map.merge(context[:opts], %{vhost: context[:vhost]})

    assert @command.banner([], vhost_opts)
      =~ ~r/Listing operator policy overrides for vhost \"#{context[:vhost]}\" \.\.\./
  end

  # Checks each element of the first policy against the expected context values
  defp assert_operator_policy_list(policies, context) do
    [policy] = policies
    assert MapSet.new(policy) == MapSet.new([name: context[:key],
                                             pattern: context[:pattern],
                                             definition: context[:value],
                                             vhost: context[:vhost],
                                             priority: context[:opts][:priority],
                                             "apply-to": context[:opts][:apply_to]])
  end
end
