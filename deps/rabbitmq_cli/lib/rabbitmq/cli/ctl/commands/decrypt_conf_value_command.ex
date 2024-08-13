## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2023 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries.  All rights reserved.

alias RabbitMQ.CLI.Core.Helpers

defmodule RabbitMQ.CLI.Ctl.Commands.DecryptConfValueCommand do
  alias RabbitMQ.CLI.Core.{DocGuide, Input}

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
  @prefix "encrypted:"

  def distribution(_), do: :none

  def merge_defaults(args, opts) do
    with_defaults =
      Map.merge(
        %{
          cipher: :rabbit_pbe.default_cipher(),
          hash: :rabbit_pbe.default_hash(),
          iterations: :rabbit_pbe.default_iterations()
        },
        opts
      )

    {args, Helpers.atomize_values(with_defaults, @atomized_keys)}
  end

  def validate(args, _) when length(args) < 1 do
    {:validation_failure, {:not_enough_args, "Please provide a value to decode and a passphrase"}}
  end

  def validate(args, _) when length(args) > 2 do
    {:validation_failure, :too_many_args}
  end

  def validate(_args, opts) do
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

  def run([value], %{cipher: cipher, hash: hash, iterations: iterations} = opts) do
    case Input.consume_single_line_string_with_prompt("Passphrase: ", opts) do
      :eof ->
        {:error, :not_enough_args}

      passphrase ->
        try do
          term_value = Helpers.evaluate_input_as_term(value)

          term_to_decrypt =
            case term_value do
              prefixed_val when is_bitstring(prefixed_val) or is_list(prefixed_val) ->
                tag_input_value_with_encrypted(prefixed_val)

              {:encrypted, _} = encrypted ->
                encrypted

              _ ->
                {:encrypted, term_value}
            end

          result = :rabbit_pbe.decrypt_term(cipher, hash, iterations, passphrase, term_to_decrypt)
          {:ok, result}
        catch
          _, _ ->
            IO.inspect(__STACKTRACE__)
            {:error,
             "Failed to decrypt the value. Things to check: is the passphrase correct? Are the cipher and hash algorithms the same as those used for encryption?"}
        end
    end
  end

  def run([value, passphrase], %{cipher: cipher, hash: hash, iterations: iterations}) do
    try do
      term_value = Helpers.evaluate_input_as_term(value)

      term_to_decrypt =
        case term_value do
          prefixed_val when is_bitstring(prefixed_val) or is_list(prefixed_val) ->
            tag_input_value_with_encrypted(prefixed_val)

          {:encrypted, _} = encrypted ->
            encrypted

          _ ->
            {:encrypted, term_value}
        end

      result = :rabbit_pbe.decrypt_term(cipher, hash, iterations, passphrase, term_to_decrypt)
      {:ok, result}
    catch
      _, _ ->
        IO.inspect(__STACKTRACE__)
        {:error,
         "Failed to decrypt the value. Things to check: is the passphrase correct? Are the cipher and hash algorithms the same as those used for encryption?"}
    end
  end

  def formatter(), do: RabbitMQ.CLI.Formatters.Erlang

  def banner(_, _) do
    "Decrypting a rabbitmq.conf string value..."
  end

  def usage,
    do: "decrypt_conf_value value passphrase [--cipher <cipher>] [--hash <hash>] [--iterations <iterations>]"

  def usage_additional() do
    [
      ["<value>", "a double-quoted rabbitmq.conf string value to decode"],
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

  defp tag_input_value_with_encrypted(value) when is_bitstring(value) or is_list(value) do
    bin_val = :rabbit_data_coercion.to_binary(value)
    untagged_val = String.replace_prefix(bin_val, @prefix, "")

    {:encrypted, untagged_val}
  end
  defp tag_input_value_with_encrypted(value) do
    {:encrypted, value}
  end
end
