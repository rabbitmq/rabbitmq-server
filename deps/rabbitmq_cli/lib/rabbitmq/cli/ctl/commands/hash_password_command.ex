## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries.  All rights reserved.

defmodule RabbitMQ.CLI.Ctl.Commands.HashPasswordCommand do
  alias RabbitMQ.CLI.Core.{Input}

  @behaviour RabbitMQ.CLI.CommandBehaviour
  use RabbitMQ.CLI.Core.MergesNoDefaults

  @hashing_algorithms %{
    "sha256" => :rabbit_password_hashing_sha256,
    "sha-256" => :rabbit_password_hashing_sha256,
    "sha512" => :rabbit_password_hashing_sha512,
    "sha-512" => :rabbit_password_hashing_sha512,
    "md5" => :rabbit_password_hashing_md5,
    "pbkdf2_sha256" => :rabbit_password_hashing_pbkdf2_sha256,
    "pbkdf2-sha256" => :rabbit_password_hashing_pbkdf2_sha256
  }

  def switches() do
    [hashing_algorithm: :string]
  end

  def run([cleartextpassword], opts) do
    hash_password(cleartextpassword, opts)
  end

  def run([], opts) do
    case Input.infer_password("Password: ", opts) do
      :eof ->
        {:error, :not_enough_args}

      password ->
        hash_password(password, opts)
    end
  end

  def hash_password(password, opts) do
    hashed_pwd =
      case Map.get(opts, :hashing_algorithm) do
        nil -> :rabbit_password.hash(password)
        alg -> :rabbit_password.hash(hashing_module(alg), password)
      end

    Base.encode64(hashed_pwd)
  end

  def validate(args, _options) when length(args) > 1 do
    {:validation_failure, :too_many_args}
  end

  def validate([""], _options) do
    {:validation_failure, {:bad_argument, "password cannot be an empty string"}}
  end

  def validate(_args, %{hashing_algorithm: alg} = _options) do
    case hashing_module(alg) do
      nil ->
        {:validation_failure, {:bad_argument, "unsupported hashing algorithm: #{alg}"}}

      _mod ->
        :ok
    end
  end

  def validate(_args, _options) do
    :ok
  end

  ## Use default output for all non-special case outputs
  use RabbitMQ.CLI.DefaultOutput

  def usage, do: "hash_password <cleartext_password> [--hashing-algorithm <algorithm>]"

  def usage_additional() do
    [
      ["<cleartext_password>", "password to hash"],
      [
        "--hashing-algorithm <algorithm>",
        "hashing algorithm to use: sha256, sha512, md5, pbkdf2_sha256"
      ]
    ]
  end

  def banner([arg], _options),
    do: "Will hash password #{arg}"

  def banner([], _options),
    do: "Will hash provided password"

  def description(), do: "Hashes a plaintext password"

  defp hashing_module(alg) when is_binary(alg) do
    Map.get(@hashing_algorithms, String.downcase(alg))
  end
end
