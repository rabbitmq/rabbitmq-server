## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2023 VMware, Inc. or its affiliates.  All rights reserved.

defmodule RabbitMQ.CLI.Ctl.Commands.HashPasswordCommand do
  alias RabbitMQ.CLI.Core.{Input}
  @behaviour RabbitMQ.CLI.CommandBehaviour
  use RabbitMQ.CLI.Core.MergesNoDefaults

  def run([cleartextpassword], %{node: node_name}) do
    hash_password(cleartextpassword, node_name)
  end

  def run([], %{node: node_name} = opts) do
    case Input.infer_password("Password: ", opts) do
      :eof ->
        {:error, :not_enough_args}

      password ->
        hash_password(password, node_name)
    end
  end

  def hash_password(password, node_name) do
    hashed_pwd =
      :rabbit_misc.rpc_call(
        node_name,
        :rabbit_password,
        :hash,
        [password]
      )

    Base.encode64(hashed_pwd)
  end

  def validate(args, _options) when length(args) > 1 do
    {:validation_failure, :too_many_args}
  end

  def validate([""], _options) do
    {:bad_argument, "password cannot be an empty string"}
  end

  def validate([_arg], _options) do
    :ok
  end

  def validate([], _options) do
    :ok
  end

  use RabbitMQ.CLI.DefaultOutput

  def usage, do: "hash_password <cleartext_password>"

  def banner([arg], _options),
    do: "Will hash password #{arg}"

  def banner([], _options),
    do: "Will hash provided password"
end
