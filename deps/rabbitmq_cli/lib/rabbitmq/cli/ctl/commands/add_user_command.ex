## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2023 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries.  All rights reserved.

defmodule RabbitMQ.CLI.Ctl.Commands.AddUserCommand do
  alias RabbitMQ.CLI.Core.{DocGuide, ExitCodes, Helpers, Input}
  import RabbitMQ.CLI.Core.Config, only: [output_less?: 1]

  @behaviour RabbitMQ.CLI.CommandBehaviour

  def switches(), do: [pre_hashed_password: :boolean]

  def merge_defaults(args, opts) do
    {args, Map.merge(%{pre_hashed_password: false}, opts)}
  end

  def validate(args, _) when length(args) < 1, do: {:validation_failure, :not_enough_args}
  def validate(args, _) when length(args) > 2, do: {:validation_failure, :too_many_args}
  # Password will be provided via standard input
  def validate([_username], _), do: :ok

  def validate(["", _], _) do
    {:validation_failure, {:bad_argument, "user cannot be an empty string"}}
  end

  def validate([_, base64_encoded_password_hash], %{pre_hashed_password: true}) do
    case Base.decode64(base64_encoded_password_hash) do
      {:ok, _password_hash} ->
        :ok

      _ ->
        {:validation_failure,
         {:bad_argument, "Could not Base64 decode provided password hash value"}}
    end
  end

  def validate([_, _], _), do: :ok

  use RabbitMQ.CLI.Core.RequiresRabbitAppRunning

  def run([username], %{node: node_name, pre_hashed_password: false} = opts) do
    # note: blank passwords are currently allowed, they make sense
    # e.g. when a user only authenticates using X.509 certificates.
    # Credential validators can be used to require passwords of a certain length
    # or following a certain pattern. This is a core server responsibility. MK.
    case Input.infer_password("Password: ", opts) do
      :eof ->
        {:error, :not_enough_args}

      password ->
        :rabbit_misc.rpc_call(
          node_name,
          :rabbit_auth_backend_internal,
          :add_user,
          [username, password, Helpers.cli_acting_user()]
        )
    end
  end

  def run([username], %{node: node_name, pre_hashed_password: true} = opts) do
    case Input.infer_password("Hashed and salted password: ", opts) do
      :eof ->
        {:error, :not_enough_args}

      base64_encoded_password_hash ->
        case Base.decode64(base64_encoded_password_hash) do
          {:ok, password_hash} ->
            :rabbit_misc.rpc_call(
              node_name,
              :rabbit_auth_backend_internal,
              :add_user_with_pre_hashed_password_sans_validation,
              [username, password_hash, Helpers.cli_acting_user()]
            )

          _ ->
            {:error, ExitCodes.exit_dataerr(),
             "Could not Base64 decode provided password hash value"}
        end
    end
  end

  def run(
        [username, base64_encoded_password_hash],
        %{node: node_name, pre_hashed_password: true} = opts
      ) do
    case Base.decode64(base64_encoded_password_hash) do
      {:ok, password_hash} ->
        :rabbit_misc.rpc_call(
          node_name,
          :rabbit_auth_backend_internal,
          :add_user_with_pre_hashed_password_sans_validation,
          [username, password_hash, Helpers.cli_acting_user()]
        )

      _ ->
        {:error, ExitCodes.exit_dataerr(), "Could not Base64 decode provided password hash value"}
    end
  end

  def run([username, password], %{node: node_name}) do
    :rabbit_misc.rpc_call(
      node_name,
      :rabbit_auth_backend_internal,
      :add_user,
      [username, password, Helpers.cli_acting_user()]
    )
  end

  def output({:error, :not_enough_args}, _) do
    {:error, ExitCodes.exit_dataerr(), "Password is not provided via argument or stdin"}
  end

  def output({:error, {:user_already_exists, username}}, %{node: node_name, formatter: "json"}) do
    {:error,
     %{"result" => "error", "node" => node_name, "message" => "User #{username} already exists"}}
  end

  def output({:error, {:user_already_exists, username}}, _) do
    {:error, ExitCodes.exit_software(), "User \"#{username}\" already exists"}
  end

  def output(:ok, %{formatter: "json", node: node_name}) do
    m = %{
      "status" => "ok",
      "node" => node_name,
      "message" =>
        "Done. Don't forget to grant the user permissions to some virtual hosts! See 'rabbitmqctl help set_permissions' to learn more."
    }

    {:ok, m}
  end

  def output(:ok, opts) do
    case output_less?(opts) do
      true ->
        :ok

      false ->
        {:ok,
         "Done. Don't forget to grant the user permissions to some virtual hosts! See 'rabbitmqctl help set_permissions' to learn more."}
    end
  end

  use RabbitMQ.CLI.DefaultOutput

  def usage, do: "add_user <username> [<password>] [<password_hash> --pre-hashed-password]"

  def usage_additional() do
    [
      ["<username>", "Self-explanatory"],
      [
        "<password>",
        "Password this user will authenticate with. Use a blank string to disable password-based authentication. Mutually exclusive with <password_hash>"
      ],
      [
        "<password_hash>",
        "A Base64-encoded password hash produced by the 'hash_password' command or a different method as described in the Passwords guide. Must be used in combination with --pre-hashed-password. Mutually exclusive with <password>"
      ],
      [
        "--pre-hashed-password",
        "Use to pass in a password hash instead of a clear text password. Disabled by default"
      ]
    ]
  end

  def usage_doc_guides() do
    [
      DocGuide.access_control(),
      DocGuide.passwords()
    ]
  end

  def help_section(), do: :user_management

  def description() do
    "Creates a new user in the internal database. This user will have no permissions for any virtual hosts by default."
  end

  def banner([username | _], _), do: "Adding user \"#{username}\" ..."
end
