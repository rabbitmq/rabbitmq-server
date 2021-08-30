## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.

# Default output implementation for plugin commands
defmodule RabbitMQ.CLI.Plugins.ErrorOutput do
  alias RabbitMQ.CLI.Core.ExitCodes

  defmacro __using__(_) do
    quote do
      def output({:error, {:enabled_plugins_mismatch, cli_path, node_path}}, opts) do
        {:error, ExitCodes.exit_dataerr(),
         "Could not update enabled plugins file at #{cli_path}: target node #{opts[:node]} uses a different path (#{
           node_path
         })"}
      end

      def output({:error, {:cannot_read_enabled_plugins_file, path, :eacces}}, _opts) do
        {:error, ExitCodes.exit_dataerr(),
         "Could not read enabled plugins file at #{path}: the file does not exist or permission was denied (EACCES)"}
      end

      def output({:error, {:cannot_read_enabled_plugins_file, path, :enoent}}, _opts) do
        {:error, ExitCodes.exit_dataerr(),
         "Could not read enabled plugins file at #{path}: the file does not exist (ENOENT)"}
      end

      def output({:error, {:cannot_write_enabled_plugins_file, path, :eacces}}, _opts) do
        {:error, ExitCodes.exit_dataerr(),
         "Could not update enabled plugins file at #{path}: the file does not exist or permission was denied (EACCES)"}
      end

      def output({:error, {:cannot_write_enabled_plugins_file, path, :enoent}}, _opts) do
        {:error, ExitCodes.exit_dataerr(),
         "Could not update enabled plugins file at #{path}: the file does not exist (ENOENT)"}
      end

      def output({:error, err}, _opts) do
        {:error, ExitCodes.exit_software(), err}
      end

      def output({:stream, stream}, _opts) do
        {:stream, stream}
      end
    end

    # quote
  end

  # defmacro
end

# defmodule
