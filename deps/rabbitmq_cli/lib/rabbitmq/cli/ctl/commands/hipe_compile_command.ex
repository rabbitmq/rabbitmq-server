## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2016-2022 VMware, Inc. or its affiliates.  All rights reserved.

defmodule RabbitMQ.CLI.Ctl.Commands.HipeCompileCommand do
  @moduledoc """
  HiPE support has been deprecated since Erlang/OTP 22 (mid-2019) and
  won't be a part of Erlang/OTP 24.

  Therefore this command is DEPRECATED and is no-op.
  """

  alias RabbitMQ.CLI.Core.{DocGuide, Validators}
  import RabbitMQ.CLI.Core.CodePath

  @behaviour RabbitMQ.CLI.CommandBehaviour

  #
  # API
  #

  def distribution(_), do: :none

  use RabbitMQ.CLI.Core.MergesNoDefaults

  def validate([], _), do: {:validation_failure, :not_enough_args}

  def validate([target_dir], opts) do
    :ok
    |> Validators.validate_step(fn ->
      case acceptable_path?(target_dir) do
        true -> :ok
        false -> {:error, {:bad_argument, "Target directory path cannot be blank"}}
      end
    end)
    |> Validators.validate_step(fn ->
      case File.dir?(target_dir) do
        true ->
          :ok

        false ->
          case File.mkdir_p(target_dir) do
            :ok ->
              :ok

            {:error, perm} when perm == :eperm or perm == :eacces ->
              {:error,
               {:bad_argument,
                "Cannot create target directory #{target_dir}: insufficient permissions"}}
          end
      end
    end)
    |> Validators.validate_step(fn -> require_rabbit(opts) end)
  end

  def validate(_, _), do: {:validation_failure, :too_many_args}

  def run([_target_dir], _opts) do
    :ok
  end
  use RabbitMQ.CLI.DefaultOutput

  def usage, do: "hipe_compile <directory>"

  def usage_additional do
    [
      ["<directory>", "Target directory for HiPE-compiled modules"]
    ]
  end

  def usage_doc_guides() do
    [
      DocGuide.configuration(),
      DocGuide.erlang_versions()
    ]
  end

  def help_section(), do: :deprecated

  def description() do
    "DEPRECATED. This command is a no-op. HiPE is no longer supported by modern Erlang versions"
  end

  def banner([_target_dir], _) do
    "This command is a no-op. HiPE is no longer supported by modern Erlang versions"
  end

  #
  # Implementation
  #

  # Accepts any non-blank path
  defp acceptable_path?(value) do
    String.length(String.trim(value)) != 0
  end
end
