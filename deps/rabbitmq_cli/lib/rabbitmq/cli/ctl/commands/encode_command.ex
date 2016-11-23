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

defmodule RabbitMQ.CLI.Ctl.Commands.EncodeCommand do
  @behaviour RabbitMQ.CLI.CommandBehaviour

  def merge_defaults(args, opts) do
    {args, Map.merge(%{
        list_ciphers: false,
        list_hashes:  false,
        cipher:       :rabbit_pbe.default_cipher(),
        hash:         :rabbit_pbe.default_hash(),
        iterations:   :rabbit_pbe.default_iterations(),
        decode:       false
        }, opts)
    }
  end
  def validate(_, %{list_ciphers: true, list_hashes: true}) do
      {:validation_failure,
       {:bad_argument, "Please list ciphers or hashes, not both."}}
  end
  def validate(args, %{list_ciphers: true, list_hashes: false}) when length(args) > 0 do
      {:validation_failure,
       {:bad_argument, :too_many_args}}
  end
  def validate(args, %{list_ciphers: false, list_hashes: true}) when length(args) > 0 do
      {:validation_failure,
       {:bad_argument, :too_many_args}}
  end
  def validate(args, %{list_ciphers: false, list_hashes: false}) when length(args) !== 2 do
      {:validation_failure,
       {:bad_argument, "Please provide a value to encode/decode and a passphrase."}}
  end
  def validate(args, %{list_ciphers: false, list_hashes: false} = opts) when length(args) === 2 do
    case {supports_cipher(opts.cipher), supports_hash(opts.hash), opts.iterations > 0} do
      {false, _, _}      -> {:validation_failure, {:bad_argument, "The requested cipher is not supported."}}
      {_, false, _}      -> {:validation_failure, {:bad_argument, "The requested hash is not supported"}}
      {_, _, false}      -> {:validation_failure, {:bad_argument, "The requested number of iterations is incorrect"}}
      {true, true, true} -> :ok
    end
  end
  def validate(_, _), do: :ok
  def switches() do
      [
        list_ciphers: :boolean,
        list_hashes: :boolean,
        cipher: :atom,
        hash: :atom,
        iterations: :integer,
        decode: :boolean
      ]
    end

  def run(_, %{list_ciphers: true}) do
    {:ok, :rabbit_pbe.supported_ciphers()}
  end

  def run(_, %{list_hashes: true}) do
    {:ok, :rabbit_pbe.supported_hashes()}
  end

  def run([value, passphrase], %{list_ciphers: false, list_hashes: false, decode: false} = opts) do
    %{:cipher => cipher, :hash => hash, :iterations => iterations} = opts
    try do
      term_value = evaluate_input_as_term(value)
      result = :rabbit_pbe.encrypt_term(cipher, hash, iterations, passphrase, term_value)
      {:ok, {:encrypted, result}}
    catch _, _ ->
      {:error, "Error during cipher operation."}
    end
  end

  def run([value, passphrase], %{list_ciphers: false, list_hashes: false, decode: true} = opts) do
    %{:cipher => cipher, :hash => hash, :iterations => iterations} = opts
    try do
      term_value = evaluate_input_as_term(value)
      term_to_decrypt = case term_value do
        {:encrypted, encrypted_term} -> encrypted_term
        _                            -> term_value
      end
      result = :rabbit_pbe.decrypt_term(cipher, hash, iterations, passphrase, term_to_decrypt)
      {:ok, result}
    catch _, _ ->
      {:error, "Error during cipher operation. Are you sure the passphrase is correct?"}
    end
  end

  def run(_, _) do
    {:error, "Incorrect usage of the encode command."}
  end

  # skip the default output (otherwise, formatting issue with Erlang lists/strings
  # which are then treated as Elixir streams and get displayed on several lines
  def output(result, opts) do
    case result do
      {:error, _} -> RabbitMQ.CLI.DefaultOutput.output(result, opts, __MODULE__)
      {:ok, _}    -> result
    end
  end

  def formatter(), do: RabbitMQ.CLI.Formatters.Erlang

  def usage, do: "encode [--decode] [value] [passphrase] [--list-ciphers] [--list-hashes] [--cipher cipher] [--hash hash] [--iterations iterations]"

  def banner(_, %{list_ciphers: true}), do: "Listing supported ciphers ..."

  def banner(_, %{list_hashes: true}), do: "Listing supported hash algorithms ..."

  def banner([_, _], %{list_ciphers: false, list_hashes: false, decode: false}) do
    "Encrypting value ..."
  end

  def banner([_, _], %{list_ciphers: false, list_hashes: false, decode: true}) do
    "Decrypting value ..."
  end

  def banner(_, _), do: "Error ..."

  defp evaluate_input_as_term(input) do
      {:ok,tokens,_end_line} = :erl_scan.string(to_charlist(input <> "."))
      {:ok,abs_form} = :erl_parse.parse_exprs(tokens)
      {:value,term_value,_bs} = :erl_eval.exprs(abs_form, :erl_eval.new_bindings())
      term_value
  end

  defp supports_cipher(cipher), do: Enum.member?(:rabbit_pbe.supported_ciphers(), cipher)

  defp supports_hash(hash), do: Enum.member?(:rabbit_pbe.supported_hashes(), hash)

end