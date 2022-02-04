## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.

defmodule RabbitMQ.CLI.Ctl.Commands.EncodeCommand do
  alias RabbitMQ.CLI.Core.{DocGuide, Helpers}

  @behaviour RabbitMQ.CLI.CommandBehaviour
  use RabbitMQ.CLI.DefaultOutput

  def switches() do
    [
      cipher: :string,
      hash: :string,
      iterations: :integer
    ]
  end
  @atomized_keys [:cipher, :hash]

  def distribution(_), do: :none

  def merge_defaults(args, opts) do
    with_defaults = Map.merge(%{
         cipher: :rabbit_pbe.default_cipher(),
         hash: :rabbit_pbe.default_hash(),
         iterations: :rabbit_pbe.default_iterations()
       }, opts)
    {args, Helpers.atomize_values(with_defaults, @atomized_keys)}
  end

  def validate(args, _) when length(args) < 2 do
    {:validation_failure, {:not_enough_args, "Please provide a value to decode and a passphrase."}}
  end

  def validate(args, _) when length(args) > 2 do
    {:validation_failure, :too_many_args}
  end

  def validate(args, opts) when length(args) === 2 do
    case {supports_cipher(opts.cipher), supports_hash(opts.hash), opts.iterations > 0} do
      {false, _, _} ->
        {:validation_failure, {:bad_argument, "The requested cipher is not supported."}}

      {_, false, _} ->
        {:validation_failure, {:bad_argument, "The requested hash is not supported"}}

      {_, _, false} ->
        {:validation_failure, {:bad_argument, "The requested number of iterations is incorrect"}}

      {true, true, true} ->
        :ok
    end
  end

  def run([value, passphrase], %{cipher: cipher, hash: hash, iterations: iterations}) do
    try do
      term_value = Helpers.evaluate_input_as_term(value)
      result = {:encrypted, _} = :rabbit_pbe.encrypt_term(cipher, hash, iterations, passphrase, term_value)
      {:ok, result}
    catch
      _, _ ->
        {:error, "Error during cipher operation."}
    end
  end

  def formatter(), do: RabbitMQ.CLI.Formatters.Erlang

  def banner([_, _], _) do
    "Encrypting value ..."
  end

  def usage, do: "encode value passphrase [--cipher <cipher>] [--hash <hash>] [--iterations <iterations>]"

  def usage_additional() do
    [
      ["<value>", "config value to encode"],
      ["<passphrase>", "passphrase to use with the config value encryption key"],
      ["--cipher <cipher>", "cipher suite to use"],
      ["--hash <hash>", "hashing function to use"],
      ["--iterations <iterations>", "number of iteration to apply"]
    ]
  end

  def usage_doc_guides() do
    [
      DocGuide.configuration()
    ]
  end

  def help_section(), do: :configuration

  def description(), do: "Encrypts a sensitive configuration value"

  #
  # Implementation
  #

  defp supports_cipher(cipher), do: Enum.member?(:rabbit_pbe.supported_ciphers(), cipher)

  defp supports_hash(hash), do: Enum.member?(:rabbit_pbe.supported_hashes(), hash)
end
