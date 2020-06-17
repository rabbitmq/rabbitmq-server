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
## Copyright (c) 2007-2020 VMware, Inc. or its affiliates.  All rights reserved.

defmodule DecodeCommandTest do
  use ExUnit.Case, async: false
  @command RabbitMQ.CLI.Ctl.Commands.DecodeCommand

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

  test "validate: providing zero or one positional argument fails", context do
    assert match?({:validation_failure, {:not_enough_args, _}},
                  @command.validate([], context[:opts]))
    assert match?({:validation_failure, {:not_enough_args, _}},
                  @command.validate(["value"], context[:opts]))
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
    passphrase = "passphrase"
    cipher = context[:opts][:cipher]
    hash = context[:opts][:hash]
    iterations = context[:opts][:iterations]
    output = {:encrypted, _encrypted} = :rabbit_pbe.encrypt_term(cipher, hash, iterations, passphrase, secret)

    {:encrypted, encrypted} = output
    # decode plain value
    assert {:ok, secret} === @command.run([format_as_erlang_term(encrypted), passphrase], context[:opts])
    # decode {encrypted, ...} tuple form
    assert {:ok, secret} === @command.run([format_as_erlang_term(output), passphrase], context[:opts])

    # wrong passphrase
    assert match?({:error, _},
                  @command.run([format_as_erlang_term(encrypted), "wrong/passphrase"], context[:opts]))
    assert match?({:error, _},
                  @command.run([format_as_erlang_term(output), "wrong passphrase"], context[:opts]))
  end

  defp format_as_erlang_term(value) do
    :io_lib.format("~p", [value]) |> :lists.flatten() |> to_string()
  end
end
