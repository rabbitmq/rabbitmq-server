## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.

defmodule JsonStreamTest do
  use ExUnit.Case, async: false

  @formatter RabbitMQ.CLI.Formatters.JsonStream

  test "format_output map with atom keys is converted to JSON object" do
    assert @formatter.format_output(%{a: :apple, b: :beer}, %{}) ==
             "{\"a\":\"apple\",\"b\":\"beer\"}"
        or @formatter.format_output(%{a: :apple, b: :beer}, %{}) ==
             "{\"b\":\"beer\",\"a\":\"apple\"}"
  end

  test "format_output map with binary keys is converted to JSON object" do
    assert @formatter.format_output(%{"a" => :apple, "b" => :beer}, %{}) ==
             "{\"a\":\"apple\",\"b\":\"beer\"}"
  end

  test "format_output empty binary is converted to empty JSON array" do
    assert @formatter.format_output("", %{}) == ""
  end
end
