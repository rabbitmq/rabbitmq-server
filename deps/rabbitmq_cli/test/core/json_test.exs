## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries.  All rights reserved.

defmodule RabbitMQ.CLI.Core.JSONTest do
  use ExUnit.Case, async: true

  alias RabbitMQ.CLI.Core.JSON

  describe "encode/1 + decode/1 round-trip" do
    test "encodes and decodes a flat map with atom keys" do
      {:ok, bin} = JSON.encode(%{a: 1, b: "two"})
      assert {:ok, %{"a" => 1, "b" => "two"}} == JSON.decode(bin)
    end

    test "encodes and decodes nested maps" do
      {:ok, bin} = JSON.encode(%{outer: %{inner: [<<"a">>, <<"b">>]}})
      assert {:ok, %{"outer" => %{"inner" => ["a", "b"]}}} == JSON.decode(bin)
    end

    test "decodes returns {:error, _} on invalid JSON" do
      assert {:error, _} = JSON.decode("not json")
    end
  end

  describe "encode/1 — proplist to JSON object" do
    test "atom-keyed proplist becomes a JSON object" do
      {:ok, bin} = JSON.encode(a: 1, b: 2)
      assert {:ok, %{"a" => 1, "b" => 2}} == JSON.decode(bin)
    end

    test "binary-keyed proplist becomes a JSON object" do
      {:ok, bin} = JSON.encode([{<<"a">>, 1}, {<<"b">>, 2}])
      assert {:ok, %{"a" => 1, "b" => 2}} == JSON.decode(bin)
    end

    test "empty list stays an empty JSON array, not an object" do
      {:ok, bin} = JSON.encode([])
      assert {:ok, []} == JSON.decode(bin)
    end

    test "list of mixed elements is encoded as a JSON array, not an object" do
      {:ok, bin} = JSON.encode([{:a, 1}, "loose"])
      assert {:ok, [["a", 1], "loose"]} == JSON.decode(bin)
    end
  end

  describe "encode/1 — bare tuple handling (regression for cluster_nodes-style values)" do
    test "a non-proplist 2-tuple value is encoded as a JSON array" do
      # `:cluster_nodes` in rabbit's environment looks like
      # `{[node1, node2], :disc}`. Before the fix, this crashed `:thoas` with
      # `function_clause` because the first element of the tuple is a list,
      # not an atom or binary, so it does not match thoas's proplist heuristic.
      input = [cluster_nodes: {[:rabbit@n1, :rabbit@n2], :disc}]
      {:ok, bin} = JSON.encode(input)
      assert {:ok, %{"cluster_nodes" => [["rabbit@n1", "rabbit@n2"], "disc"]}} ==
               JSON.decode(bin)
    end

    test "a 3-tuple value is encoded as a 3-element JSON array" do
      {:ok, bin} = JSON.encode(point: {1, 2, 3})
      assert {:ok, %{"point" => [1, 2, 3]}} == JSON.decode(bin)
    end

    test "a deeply nested non-proplist tuple is encoded as a JSON array" do
      {:ok, bin} = JSON.encode(outer: %{inner: {1, 2}})
      assert {:ok, %{"outer" => %{"inner" => [1, 2]}}} == JSON.decode(bin)
    end
  end

  describe "encode/1 — Erlang string and port number heuristics" do
    test "an Erlang charlist is encoded as a JSON string" do
      {:ok, bin} = JSON.encode(host: ~c"localhost")
      assert {:ok, %{"host" => "localhost"}} == JSON.decode(bin)
    end

    test "a single integer port stays a one-element JSON array" do
      {:ok, bin} = JSON.encode(tcp_listeners: [5672])
      assert {:ok, %{"tcp_listeners" => [5672]}} == JSON.decode(bin)
    end

    test "two integer ports stay a two-element JSON array" do
      {:ok, bin} = JSON.encode(tcp_listeners: [5672, 5683])
      assert {:ok, %{"tcp_listeners" => [5672, 5683]}} == JSON.decode(bin)
    end

    test "a list of binaries is preserved" do
      {:ok, bin} = JSON.encode(files: [<<"a.conf">>, <<"b.conf">>])
      assert {:ok, %{"files" => ["a.conf", "b.conf"]}} == JSON.decode(bin)
    end

    # Documented footgun: an integer list whose values are valid Unicode
    # codepoints is treated as an Erlang charlist and coerced to a binary.
    # The `[5672]` and `[5672, 5683]` special cases above exist precisely to
    # avoid this for typical port-number values.
    test "an integer list with small values is coerced to a string (charlist)" do
      {:ok, bin} = JSON.encode(%{xs: [1, 2, 3]})
      assert {:ok, %{"xs" => <<1, 2, 3>>}} == JSON.decode(bin)
    end
  end

  describe "encode/1 — Erlang-specific terms become readable strings" do
    test "a pid is encoded as a Pid(...) string" do
      {:ok, bin} = JSON.encode(%{p: self()})
      assert {:ok, %{"p" => "Pid(" <> _}} = JSON.decode(bin)
    end

    test "a reference is encoded as a Ref(...) string" do
      {:ok, bin} = JSON.encode(%{r: make_ref()})
      assert {:ok, %{"r" => "Ref(" <> _}} = JSON.decode(bin)
    end

    test "a function is encoded as Fun()" do
      {:ok, bin} = JSON.encode(%{f: fn -> :ok end})
      assert {:ok, %{"f" => "Fun()"}} == JSON.decode(bin)
    end

    test "a binary with invalid UTF-8 falls back to Base64" do
      invalid = <<0xFF, 0xFE>>
      {:ok, bin} = JSON.encode(%{b: invalid})
      assert {:ok, %{"b" => encoded}} = JSON.decode(bin)
      assert encoded == Base.encode64(invalid)
    end
  end
end
