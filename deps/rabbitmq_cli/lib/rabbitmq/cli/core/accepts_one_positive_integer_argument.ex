## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries.  All rights reserved.

# Should be used by commands that require rabbit app to be stopped
# but need no other execution environment validators.
defmodule RabbitMQ.CLI.Core.AcceptsOnePositiveIntegerArgument do
  defmacro __using__(_) do
    quote do
      def validate(args, _) when length(args) == 0 do
        {:validation_failure, :not_enough_args}
      end

      def validate(args, _) when length(args) > 1 do
        {:validation_failure, :too_many_args}
      end

      def validate([value], _) when is_integer(value) do
        :ok
      end

      def validate([value], _) do
        case Integer.parse(value) do
          {n, _} when n >= 1 -> :ok
          :error -> {:validation_failure, {:bad_argument, "Argument must be a positive integer"}}
        end
      end

      def validate(_, _), do: :ok
    end
  end
end
