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


defmodule HelpCommandTest do
  use ExUnit.Case, async: false
  import ExUnit.CaptureIO

  setup_all do
    :ok
  end

  test "basic usage info is printed" do
    assert capture_io(fn -> HelpCommand.help end) =~ ~r/Default node is \"rabbit@server\"/
  end

  test "Command info is printed" do
    assert capture_io(fn -> HelpCommand.help end) =~ ~r/Commands:\n/
    assert capture_io(fn -> HelpCommand.help end) =~ ~r/\tstatus\n/
    assert capture_io(fn -> HelpCommand.help end) =~ ~r/\tenvironment\n/
  end
end
