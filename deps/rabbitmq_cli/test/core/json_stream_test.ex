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
## Copyright (c) 2019 Pivotal Software, Inc.  All rights reserved.

defmodule JsonStreamTest do
  use ExUnit.Case, async: false

  @formatter RabbitMQ.CLI.Formatters.JsonStream

  test "format_output map with atom keys is converted to JSON object" do
    assert @formatter.format_output(%{a: :apple, b: :beer}, %{}) == "{\"a\":\"apple\",\"b\":\"beer\"}"
  end

  test "format_output map with binary keys is converted to JSON object" do
    assert @formatter.format_output(%{"a" => :apple, "b" => :beer}, %{}) == "{\"a\":\"apple\",\"b\":\"beer\"}"
  end

  test "format_output empty binary is converted to empty JSON array" do
    assert @formatter.format_output("", %{}) == ""
  end

end
