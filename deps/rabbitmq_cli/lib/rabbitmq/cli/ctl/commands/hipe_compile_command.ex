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
## The Initial Developer of the Original Code is Pivotal Software, Inc.
## Copyright (c) 2016-2017 Pivotal Software, Inc.  All rights reserved.

defmodule RabbitMQ.CLI.Ctl.Commands.HipeCompileCommand do
  alias RabbitMQ.CLI.Core.Helpers, as: Helpers
  
  @behaviour RabbitMQ.CLI.CommandBehaviour
  use RabbitMQ.CLI.DefaultOutput

  def merge_defaults(args, opts) do
    {args, opts}
  end

  def usage, do: "hipe_compile <directory>"

  def validate([], _),  do: {:validation_failure, :not_enough_args}
  def validate([_], opts) do
    :ok
    |> Helpers.validate_step(fn() -> Helpers.require_rabbit(opts) end)
  end
  def validate(_, _),   do: {:validation_failure, :too_many_args}

  def run([target_dir], _opts) do
    case :application.load(:rabbit) do
      :ok ->
        case :rabbit_hipe.can_hipe_compile() do
          true ->
            {:ok, _, _} = :rabbit_hipe.compile_to_directory(target_dir)
            :ok
          false ->
            {:error, "HiPE compilation is not supported"}
        end
      {:error, {'no such file or directory', 'rabbit.app'}} ->
        {:error, "Failed to load RabbitMQ server app metadata. Cannot proceed with HiPE compilation."}
      {:error, _reason} ->
        {:error, "Failed to load RabbitMQ modules or app metadata. Cannot proceed with HiPE compilation."}
    end

  end

  def banner([target_dir], _) do
    "Will pre-compile RabbitMQ server modules with HiPE to #{target_dir} ..."
  end
end
