## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries.  All rights reserved.

defmodule RabbitMQ.CLI.Queues.Commands.PeekCommandTest do
  use ExUnit.Case, async: false
  import TestHelper

  @command RabbitMQ.CLI.Queues.Commands.PeekCommand
  @invalid_position {:validation_failure, "position value must be a positive integer"}

  setup_all do
    RabbitMQ.CLI.Core.Distribution.start()

    :ok
  end

  setup context do
    {:ok,
     opts: %{
       node: get_rabbit_hostname(),
       timeout: context[:test_timeout] || 30000
     }}
  end

  test "validate: treats no arguments as a failure" do
    assert @command.validate([], %{}) == {:validation_failure, :not_enough_args}
  end

  test "validate: treats a single positional argument as a failure" do
    assert @command.validate(["quorum-queue-a"], %{}) == {:validation_failure, :not_enough_args}
  end

  test "validate: when two or more arguments are provided, returns a failure" do
    assert @command.validate(["quorum-queue-a", "1"], %{}) == :ok

    assert @command.validate(["quorum-queue-a", "extra-arg", "another-extra-arg"], %{}) ==
             {:validation_failure, :too_many_args}
  end

  test "validate: when position is a negative number, returns a failure" do
    assert @command.validate(["quorum-queue-a", "-1"], %{}) == @invalid_position
  end

  test "validate: when position is zero, returns a failure" do
    assert @command.validate(["quorum-queue-a", "0"], %{}) == @invalid_position
  end

  test "validate: when position cannot be parsed to an integer, returns a failure" do
    assert @command.validate(["quorum-queue-a", "third"], %{}) == @invalid_position
  end

  @tag test_timeout: 3000
  test "run: targeting an unreachable node throws a badrpc" do
    assert match?(
             {:badrpc, _},
             @command.run(
               ["quorum-queue-a", "1"],
               %{node: :jake@thedog, vhost: "/", timeout: 200}
             )
           )
  end

  test "output: short values are displayed as is" do
    msg = [{"exchange", ""}, {"header.x-count", 5}, {"routing_keys", ["qq"]}]

    assert @command.output({:ok, msg}, %{}) ==
             {:stream,
              [
                [{"keys", "exchange"}, {"values", ""}],
                [{"keys", "header.x-count"}, {"values", 5}],
                [{"keys", "routing_keys"}, {"values", ["qq"]}]
              ]}
  end

  test "output: long binary values are truncated to 120 characters and include their size" do
    value = String.duplicate("a", 2048)

    assert @command.output({:ok, [{"header.x-token", value}]}, %{}) ==
             {:stream,
              [
                [
                  {"keys", "header.x-token"},
                  {"values", String.duplicate("a", 120) <> "... (2048 bytes)"}
                ]
              ]}
  end

  test "output: long multi-byte UTF-8 values are truncated on character boundaries" do
    value = String.duplicate("é", 200)

    {:stream, [[_, {"values", truncated}]]} =
      @command.output({:ok, [{"header.x-text", value}]}, %{})

    assert truncated == String.duplicate("é", 120) <> "... (400 bytes)"
  end

  test "output: long nested values are truncated to 120 characters" do
    value = for n <- 1..50, do: {"key-#{n}", :longstr, "value-#{n}"}

    {:stream, [[_, {"values", truncated}]]} =
      @command.output({:ok, [{"header.x-table", value}]}, %{})

    assert String.length(truncated) == 123
    assert String.ends_with?(truncated, "...")
  end

  test "output: values are not truncated with the JSON formatter" do
    value = String.duplicate("a", 2048)

    assert @command.output({:ok, [{"header.x-token", value}]}, %{formatter: "json"}) ==
             {:ok, %{"result" => "ok", "message" => %{"header.x-token" => value}}}
  end
end
