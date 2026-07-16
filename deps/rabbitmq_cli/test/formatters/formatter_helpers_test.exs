## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries.  All rights reserved.

defmodule FormatterHelpersTest do
  use ExUnit.Case, async: true

  alias RabbitMQ.CLI.Formatters.FormatterHelpers

  test "format_info_item renders an IPv4 address tuple as a string" do
    assert to_string(FormatterHelpers.format_info_item({127, 0, 0, 1})) == "127.0.0.1"
  end

  test "format_info_item renders a Unix domain socket path" do
    assert to_string(FormatterHelpers.format_info_item({:local, ~c"/tmp/rmq.sock"})) ==
             "/tmp/rmq.sock"

    assert to_string(FormatterHelpers.format_info_item({:local, "/tmp/rmq.sock"})) ==
             "/tmp/rmq.sock"
  end

  test "format_info_item renders an accepted Unix domain socket peer as an empty string" do
    assert to_string(FormatterHelpers.format_info_item({:local, ""})) == ""
  end
end
