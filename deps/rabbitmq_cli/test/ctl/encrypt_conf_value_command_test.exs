## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2024 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries.  All rights reserved.

defmodule EncryptConfValueCommandTest do
  use ExUnit.Case, async: false

  @command RabbitMQ.CLI.Ctl.Commands.EncryptConfValueCommand

  setup _context do
    {:ok,
     opts: %{
       cipher: :rabbit_pbe.default_cipher(),
       hash: :rabbit_pbe.default_hash(),
       iterations: :rabbit_pbe.default_iterations()
     }}
  end

  test "validate: providing exactly 2 positional arguments passes", context do
    assert :ok == @command.validate(["value", "secret"], context[:opts])
  end

  test "validate: providing zero or one positional argument passes", context do
    assert :ok == @command.validate([], context[:opts])
    assert :ok == @command.validate(["value"], context[:opts])
  end

  test "validate: providing three or more positional argument fails", context do
    assert match?(
             {:validation_failure, :too_many_args},
             @command.validate(["value", "secret", "incorrect"], context[:opts])
           )
  end

  test "validate: hash and cipher must be supported", context do
    assert match?(
             {:validation_failure, {:bad_argument, _}},
             @command.validate(
               ["value", "secret"],
               Map.merge(context[:opts], %{cipher: :funny_cipher})
             )
           )

    assert match?(
             {:validation_failure, {:bad_argument, _}},
             @command.validate(
               ["value", "secret"],
               Map.merge(context[:opts], %{hash: :funny_hash})
             )
           )

    assert match?(
             {:validation_failure, {:bad_argument, _}},
             @command.validate(
               ["value", "secret"],
               Map.merge(context[:opts], %{cipher: :funny_cipher, hash: :funny_hash})
             )
           )

    assert :ok == @command.validate(["value", "secret"], context[:opts])
  end

  test "validate: number of iterations must greater than 0", context do
    assert match?(
             {:validation_failure, {:bad_argument, _}},
             @command.validate(["value", "secret"], Map.merge(context[:opts], %{iterations: 0}))
           )

    assert match?(
             {:validation_failure, {:bad_argument, _}},
             @command.validate(["value", "secret"], Map.merge(context[:opts], %{iterations: -1}))
           )

    assert :ok == @command.validate(["value", "secret"], context[:opts])
  end
end
