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
## Copyright (c) 2007-2019 Pivotal Software, Inc.  All rights reserved.

alias RabbitMQ.CLI.Core.Helpers

defmodule RabbitMQ.CLI.Ctl.Commands.DecodeCommand do
  alias RabbitMQ.CLI.Core.DocGuide

  @behaviour RabbitMQ.CLI.CommandBehaviour
  use RabbitMQ.CLI.DefaultOutput

  def switches() do
    [
      cipher: :atom,
      hash: :atom,
      iterations: :integer
    ]
  end

  def distribution(_), do: :none

  def merge_defaults(args, opts) do
    {args,
     Map.merge(
       %{
         cipher: :rabbit_pbe.default_cipher(),
         hash: :rabbit_pbe.default_hash(),
         iterations: :rabbit_pbe.default_iterations()
       },
       opts
     )}
  end

  def validate(args, _) when length(args) < 2 do
    {:validation_failure, {:bad_argument, "Please provide a value to decode and a passphrase"}}
  end

  def validate(args, _) when length(args) > 2 do
    {:validation_failure, :too_many_args}
  end

  def validate(args, opts) when length(args) === 2 do
    case {supports_cipher(opts.cipher), supports_hash(opts.hash), opts.iterations > 0} do
      {false, _, _} ->
        {:validation_failure, {:bad_argument, "The requested cipher is not supported"}}

      {_, false, _} ->
        {:validation_failure, {:bad_argument, "The requested hash is not supported"}}

      {_, _, false} ->
        {:validation_failure,
         {:bad_argument,
          "The requested number of iterations is incorrect (must be a positive integer)"}}

      {true, true, true} ->
        :ok
    end
  end

  def run([value, passphrase], %{cipher: cipher, hash: hash, iterations: iterations}) do
    try do
      term_value = Helpers.evaluate_input_as_term(value)

      term_to_decrypt =
        case term_value do
          {:encrypted, encrypted_term} -> encrypted_term
          _ -> term_value
        end

      result = :rabbit_pbe.decrypt_term(cipher, hash, iterations, passphrase, term_to_decrypt)
      {:ok, result}
    catch
      _, _ ->
        {:error,
         "Failed to decrypt the value. Things to check: is the passphrase correct? Are the cipher and hash algorithms the same as those used for encryption?"}
    end
  end

  def formatter(), do: RabbitMQ.CLI.Formatters.Erlang

  def banner([_, _], _) do
    "Decrypting value ..."
  end

  def usage, do: "decode value passphrase [--cipher <cipher>] [--hash <hash>] [--iterations <iterations>]"

  def usage_additional() do
    [
      ["<value>", "config value to decode"],
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

  def description(), do: "Decrypts an encrypted configuration value"

  #
  # Implementation
  #

  defp supports_cipher(cipher), do: Enum.member?(:rabbit_pbe.supported_ciphers(), cipher)

  defp supports_hash(hash), do: Enum.member?(:rabbit_pbe.supported_hashes(), hash)
end
