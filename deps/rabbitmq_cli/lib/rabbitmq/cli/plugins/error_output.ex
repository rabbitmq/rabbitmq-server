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
