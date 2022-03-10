## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2020 VMware, Inc. or its affiliates.  All rights reserved.

defmodule EncodeCommandTest do
  use ExUnit.Case, async: false

  @command RabbitMQ.CLI.Ctl.Commands.EncodeCommand

  setup _context do
    {:ok, opts: %{
      cipher: :rabbit_pbe.default_cipher,
      hash: :rabbit_pbe.default_hash,
      iterations: :rabbit_pbe.default_iterations
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
    assert match?({:validation_failure, :too_many_args},
                  @command.validate(["value", "secret", "incorrect"], context[:opts]))
  end

  test "validate: hash and cipher must be supported", context do
    assert match?(
      {:validation_failure, {:bad_argument, _}},
      @command.validate(["value", "secret"], Map.merge(context[:opts], %{cipher: :funny_cipher}))
    )
    assert match?(
      {:validation_failure, {:bad_argument, _}},
      @command.validate(["value", "secret"], Map.merge(context[:opts], %{hash: :funny_hash}))
    )
    assert match?(
      {:validation_failure, {:bad_argument, _}},
      @command.validate(["value", "secret"], Map.merge(context[:opts], %{cipher: :funny_cipher, hash: :funny_hash}))
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

  test "run: encrypt/decrypt", context do
    # an Erlang list/bitstring
    encrypt_decrypt(to_charlist("foobar"), context)
    # a binary
    encrypt_decrypt("foobar", context)
    # a tuple
    encrypt_decrypt({:password, "secret"}, context)
  end

  defp encrypt_decrypt(secret, context) do
    secret_as_erlang_term = format_as_erlang_term(secret)
    passphrase = "passphrase"

    cipher = context[:opts][:cipher]
    hash = context[:opts][:hash]
    iterations = context[:opts][:iterations]

    {:ok, output} = @command.run([secret_as_erlang_term, passphrase], context[:opts])
    {:encrypted, encrypted} = output
    # decode plain value
    assert secret === :rabbit_pbe.decrypt_term(cipher, hash, iterations, passphrase, {:plaintext, secret})
    # decode {encrypted, ...} tuple form
    assert secret === :rabbit_pbe.decrypt_term(cipher, hash, iterations, passphrase, {:encrypted, encrypted})
  end

  defp format_as_erlang_term(value) do
    :io_lib.format("~p", [value]) |> :lists.flatten() |> to_string()
  end
end
