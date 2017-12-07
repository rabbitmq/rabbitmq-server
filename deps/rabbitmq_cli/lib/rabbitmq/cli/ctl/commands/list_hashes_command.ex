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
## Copyright (c) 2007-2017 Pivotal Software, Inc.  All rights reserved.

defmodule RabbitMQ.CLI.Ctl.Commands.ListHashesCommand do
  @behaviour RabbitMQ.CLI.CommandBehaviour
  use RabbitMQ.CLI.DefaultOutput

  def merge_defaults(args, opts) do
    {args, opts}
  end
  def validate(args, _) when length(args) > 0 do
      {:validation_failure,
       {:bad_argument, :too_many_args}}
  end
  def validate(_, _), do: :ok

  def distribution(_), do: :none

  def run(_, _) do
    {:ok, :rabbit_pbe.supported_hashes()}
  end

  def formatter(), do: RabbitMQ.CLI.Formatters.Erlang

  def usage, do: "list_hashes"

  def banner(_, _), do: "Listing supported hash algorithms ..."

end
