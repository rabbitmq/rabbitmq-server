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
## The Initial Developer of the Original Code is Pivotal Software, Inc.
## Copyright (c) 2016-2017 Pivotal Software, Inc.  All rights reserved.

defmodule RabbitMQ.CLI.Ctl.Commands.HipeCompileCommand do
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

  def run([target_dir], _opts) do
    Code.ensure_loaded(:rabbit_hipe)
    hipe_compile(String.trim(target_dir))
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

  def help_section(), do: :operations

  def description() do
    "Only exists for backwards compatibility. HiPE support has been dropped starting with Erlang 22. "
    <> "Do not use"
  end

  def banner([target_dir], _) do
    "Will pre-compile RabbitMQ server modules with HiPE to #{target_dir} ..."
  end

  #
  # Implementation
  #

  # Accepts any non-blank path
  defp acceptable_path?(value) do
    String.length(String.trim(value)) != 0
  end

  defp hipe_compile(target_dir) do
    case :rabbit_hipe.can_hipe_compile() do
      true ->
        case :rabbit_hipe.compile_to_directory(target_dir) do
          {:ok, _, _} -> :ok
          {:ok, :already_compiled} -> {:ok, "already compiled"}
          {:error, message} -> {:error, message}
        end

      false ->
        {:error, "HiPE compilation is not supported"}
    end
  end
end
