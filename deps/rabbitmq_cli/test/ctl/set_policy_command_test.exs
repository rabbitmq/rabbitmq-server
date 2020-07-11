## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2020 VMware, Inc. or its affiliates.  All rights reserved.


defmodule SetPolicyCommandTest do
  use ExUnit.Case, async: false
  import TestHelper

  @command RabbitMQ.CLI.Ctl.Commands.SetPolicyCommand

  @vhost "test1"
  @root   "/"
  @key "federate"
  @pattern "^fed\."
  @value "{\"federation-upstream-set\":\"all\"}"
  @apply_to "all"
  @priority 0

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
        node: get_rabbit_hostname(),
        vhost: "/",
        apply_to: @apply_to,
        priority: @priority
      }
    }
  end

  @tag pattern: @pattern, key: @key, value: @value, vhost: @root
  test "merge_defaults: a well-formed command with no vhost runs against the default" do
    assert match?({_, %{vhost: "/"}}, @command.merge_defaults([], %{}))
  end

  test "merge_defaults: does not change defined vhost" do
    assert match?({[], %{vhost: "test_vhost"}}, @command.merge_defaults([], %{vhost: "test_vhost"}))
  end

  test "merge_defaults: default apply_to is \"all\"" do
    assert match?({_, %{apply_to: "all"}}, @command.merge_defaults([], %{}))
    assert match?({_, %{apply_to: "custom"}}, @command.merge_defaults([], %{apply_to: "custom"}))
  end

  test "merge_defaults: default priority is 0" do
    assert match?({_, %{priority: 0}}, @command.merge_defaults([], %{}))
    assert match?({_, %{priority: 3}}, @command.merge_defaults([], %{priority: 3}))
  end

  test "validate: providing too few arguments fails validation" do
    assert @command.validate([], %{}) == {:validation_failure, :not_enough_args}
    assert @command.validate(["insufficient"], %{}) == {:validation_failure, :not_enough_args}
    assert @command.validate(["not", "enough"], %{}) == {:validation_failure, :not_enough_args}
  end

  test "validate: providing too many arguments fails validation" do
    assert @command.validate(["this", "is", "too", "many"], %{}) == {:validation_failure, :too_many_args}
  end

  @tag pattern: @pattern, key: @key, value: @value, vhost: @vhost
  test "run: a well-formed, host-specific command returns okay", context do
    vhost_opts = Map.merge(context[:opts], %{vhost: context[:vhost]})

    assert @command.run(
      [context[:key], context[:pattern], context[:value]],
      vhost_opts
    ) == :ok

    assert_policy_fields(context)
  end

  test "run: an unreachable node throws a badrpc" do
    opts = %{node: :jake@thedog, vhost: "/", priority: 0, apply_to: "all", timeout: 200}

    assert match?({:badrpc, _}, @command.run([@key, @pattern, @value], opts))
  end

  @tag pattern: @pattern, key: @key, value: @value, vhost: "bad-vhost"
  test "run: providing a non-existent vhost reports an error", context do
    vhost_opts = Map.merge(context[:opts], %{vhost: context[:vhost]})

    assert @command.run(
      [context[:key], context[:pattern], context[:value]],
      vhost_opts
    ) == {:error, {:no_such_vhost, context[:vhost]}}
  end

  @tag pattern: @pattern, key: @key, value: "bad-value", vhost: @root
  test "run: an invalid value returns a JSON decoding error", context do
    assert match?({:error_string, _},
      @command.run([context[:key], context[:pattern], context[:value]],
        context[:opts]))

    assert list_policies(context[:vhost]) == []
  end

  @tag pattern: @pattern, key: @key, value: "{\"foo\":\"bar\"}", vhost: @root
  test "run: invalid policy returns an error", context do
    assert @command.run(
      [context[:key], context[:pattern], context[:value]],
      context[:opts]
    ) == {:error_string, 'Validation failed\n\n[{<<"foo">>,<<"bar">>}] are not recognised policy settings\n'}

    assert list_policies(context[:vhost]) == []
  end

  @tag pattern: @pattern, key: @key, value: "{}", vhost: @root
  test "run: an empty JSON object value returns an error", context do
    assert @command.run(
      [context[:key], context[:pattern], context[:value]],
      context[:opts]
    ) == {:error_string, 'Validation failed\n\nno policy provided\n'}

    assert list_policies(context[:vhost]) == []
  end

  @tag pattern: @pattern, key: @key, value: @value, vhost: @vhost
  test "banner", context do
    vhost_opts = Map.merge(context[:opts], %{vhost: context[:vhost]})

    assert @command.banner([context[:key], context[:pattern], context[:value]], vhost_opts)
      == "Setting policy \"#{context[:key]}\" for pattern \"#{context[:pattern]}\" to \"#{context[:value]}\" with priority \"#{context[:opts][:priority]}\" for vhost \"#{context[:vhost]}\" \.\.\."
  end

  @tag pattern: "ha_", key: "ha_policy_test", vhost: @vhost
  test "ha policy validation", context do
    vhost_opts = Map.merge(context[:opts], %{vhost: context[:vhost]})
    context = Map.put(context, :opts, vhost_opts)
    pass_validation(context, "{\"ha-mode\":\"all\"}")
    fail_validation(context, "{\"ha-mode\":\"made_up\"}")

    fail_validation(context, "{\"ha-mode\":\"nodes\"}")
    fail_validation(context, "{\"ha-mode\":\"nodes\",\"ha-params\":2}")
    fail_validation(context, "{\"ha-mode\":\"nodes\",\"ha-params\":[\"a\",2]}")
    pass_validation(context, "{\"ha-mode\":\"nodes\",\"ha-params\":[\"a\",\"b\"]}")
    fail_validation(context, "{\"ha-params\":[\"a\",\"b\"]}")

    fail_validation(context, "{\"ha-mode\":\"exactly\"}")
    fail_validation(context, "{\"ha-mode\":\"exactly\",\"ha-params\":[\"a\",\"b\"]}")
    pass_validation(context, "{\"ha-mode\":\"exactly\",\"ha-params\":2}")
    fail_validation(context, "{\"ha-params\":2}")

    pass_validation(context, "{\"ha-mode\":\"all\",\"ha-sync-mode\":\"manual\"}")
    pass_validation(context, "{\"ha-mode\":\"all\",\"ha-sync-mode\":\"automatic\"}")
    fail_validation(context, "{\"ha-mode\":\"all\",\"ha-sync-mode\":\"made_up\"}")
    fail_validation(context, "{\"ha-sync-mode\":\"manual\"}")
    fail_validation(context, "{\"ha-sync-mode\":\"automatic\"}")
  end

  @tag pattern: "ha_", key: "ha_policy_test", vhost: @vhost
  test "queue master locator policy validation", context do
    vhost_opts = Map.merge(context[:opts], %{vhost: context[:vhost]})
    context = Map.put(context, :opts, vhost_opts)
    pass_validation(context, "{\"queue-master-locator\":\"min-masters\"}")
    pass_validation(context, "{\"queue-master-locator\":\"client-local\"}")
    pass_validation(context, "{\"queue-master-locator\":\"random\"}")
    fail_validation(context, "{\"queue-master-locator\":\"made_up\"}")
  end

  @tag pattern: "ha_", key: "ha_policy_test", vhost: @vhost
  test "queue modes policy validation", context do
    vhost_opts = Map.merge(context[:opts], %{vhost: context[:vhost]})
    context = Map.put(context, :opts, vhost_opts)
    pass_validation(context, "{\"queue-mode\":\"lazy\"}")
    pass_validation(context, "{\"queue-mode\":\"default\"}")
    fail_validation(context, "{\"queue-mode\":\"wrong\"}")
  end

  def pass_validation(context, value) do
    assert @command.run(
      [context[:key], context[:pattern], value],
      context[:opts]
    ) == :ok
    assert_policy_fields(Map.merge(context, %{value: value}))
  end

  def fail_validation(context, value) do
    result = @command.run(
      [context[:key], context[:pattern], value],
      context[:opts]
    )
    assert {:error_string, _} = result
    {:error_string, msg} = result
    assert "Validation failed"<>_ = to_string(msg)
  end

  # Checks each element of the first policy against the expected context values
  defp assert_policy_fields(context) do
    result_policy = context[:vhost] |> list_policies |> List.first
    assert result_policy[:definition] == context[:value]
    assert result_policy[:vhost] == context[:vhost]
    assert result_policy[:pattern] == context[:pattern]
    assert result_policy[:name] == context[:key]
  end
end
