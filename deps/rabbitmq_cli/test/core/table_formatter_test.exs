## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries.  All rights reserved.

defmodule TableFormatterTest do
  use ExUnit.Case, async: false

  @formatter RabbitMQ.CLI.Formatters.Table

  test "format_output tab-separates keyword values" do
    assert @formatter.format_output([a: :apple, b: :beer], %{}) == ["a\tb", "apple\tbeer"]

    assert @formatter.format_output([a: :apple, b: :beer, c: 1], %{}) == [
             "a\tb\tc",
             "apple\tbeer\t1"
           ]

    assert @formatter.format_output([a: "apple", b: ~c"beer", c: 1], %{}) == [
             "a\tb\tc",
             "apple\t\"beer\"\t1"
           ]
  end

  test "format_stream tab-separates keyword values" do
    assert @formatter.format_stream(
             [[a: :apple, b: :beer, c: 1], [a: "aadvark", b: ~c"bee", c: 2]],
             %{}
           )
           |> Enum.to_list() ==
             ["a\tb\tc", "apple\tbeer\t1", "aadvark\t\"bee\"\t2"]
  end

  test "format_output formats non-string values with inspect recursively" do
    assert @formatter.format_output(
             %{
               a: :apple,
               b: "beer",
               c: {:carp, "fish"},
               d: [door: :way],
               e: %{elk: "horn", for: :you}
             },
             %{}
           ) ==
             [
               "a\tb\tc\td\te",
               "apple\tbeer\t{carp, fish}\t[{door, way}]\t\#{elk => horn, for => you}"
             ]

    assert @formatter.format_output(
             %{
               a: :apple,
               b: "beer",
               c: {:carp, {:small, :fish}},
               d: [door: {:way, "big"}],
               e: %{elk: [horn: :big]}
             },
             %{}
           ) ==
             [
               "a\tb\tc\td\te",
               "apple\tbeer\t{carp, {small, fish}}\t[{door, {way, big}}]\t\#{elk => [{horn, big}]}"
             ]
  end
end
