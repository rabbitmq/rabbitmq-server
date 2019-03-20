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

# Should be used by commands that require rabbit app to be stopped
# but need no other execution environment validators.
defmodule RabbitMQ.CLI.Core.AcceptsTwoPositionalArguments do
  defmacro __using__(_) do
    quote do
      def validate(args, _) when length(args) < 2 do
        {:validation_failure, :not_enough_args}
      end

      def validate(args, _) when length(args) > 2 do
        {:validation_failure, :too_many_args}
      end

      # Note: this will accept everything, so it must be the
      # last validation clause defined!
      def validate([_, _], _), do: :ok
    end
  end
end
