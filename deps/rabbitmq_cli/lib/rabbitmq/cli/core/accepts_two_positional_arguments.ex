## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.

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
