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
## Copyright (c) 2007-2017 Pivotal Software, Inc.  All rights reserved.

defmodule ListHashesCommandTest do
  use ExUnit.Case, async: false
  @command RabbitMQ.CLI.Ctl.Commands.ListHashesCommand

  test "validate: providing arguments when listing hashes is reported as invalid", _context do
    assert match?(
      {:validation_failure, {:bad_argument, :too_many_args}},
      @command.validate(["value"], %{})
    )
  end

  test "run: list hashes", _context do
    assert match?(
      {:ok, _},
      @command.run([], %{})
    )
  end
end