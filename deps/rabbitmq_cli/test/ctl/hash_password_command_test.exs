## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries.  All rights reserved.

defmodule HashPasswordCommandTest do
  use ExUnit.Case, async: false
  import TestHelper

  @command RabbitMQ.CLI.Ctl.Commands.HashPasswordCommand

  setup_all do
    RabbitMQ.CLI.Core.Distribution.start()
    :ok
  end

  setup context do
    on_exit(context, fn -> delete_user(context[:user]) end)
    {:ok, opts: %{node: get_rabbit_hostname()}}
  end

  test "validate: too many arguments", context do
    assert @command.validate(["foo", "bar"], context[:opts]) ==
             {:validation_failure, :too_many_args}
  end

  test "validate: empty string", context do
    assert @command.validate([""], context[:opts]) ==
             {:validation_failure, {:bad_argument, "password cannot be an empty string"}}
  end

  test "validate: unsupported hashing algorithm", context do
    opts = Map.put(context[:opts], :hashing_algorithm, "sha1")

    assert @command.validate(["foo"], opts) ==
             {:validation_failure, {:bad_argument, "unsupported hashing algorithm: sha1"}}
  end

  test "validate: supported hashing algorithm", context do
    opts = Map.put(context[:opts], :hashing_algorithm, "sha512")
    assert @command.validate(["foo"], opts) == :ok
  end

  @tag user: "someone", password: "hashed_password"
  test "run: successfully create user with a hashed password from cli cmd", context do
    hashed_pwd = @command.run([context[:password]], context[:opts])
    add_user_hashed_password(context[:user], hashed_pwd)
    assert {:ok, _} = authenticate_user(context[:user], context[:password])
  end

  @tag password: "hashed_password"
  test "run: hashes the password with the requested --hashing-algorithm", context do
    opts = Map.put(context[:opts], :hashing_algorithm, "sha512")
    hashed_pwd = @command.run([context[:password]], opts)
    <<salt::binary-size(4), hash::binary>> = Base.decode64!(hashed_pwd)

    assert hash == :crypto.hash(:sha512, salt <> context[:password])
  end

  @tag user: "someone", password: "hashed_password"
  test "run: Create user with a hashed password from cli cmd, use hashed pwd as cleartest password",
       context do
    hashed_pwd = @command.run([context[:password]], context[:opts])
    add_user_hashed_password(context[:user], hashed_pwd)
    assert {:refused, _, _, _} = authenticate_user(context[:user], hashed_pwd)
  end
end
