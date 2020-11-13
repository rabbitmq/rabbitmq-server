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

defmodule RabbitMQ.CLI.CommandBehaviour do
  alias RabbitMQ.CLI.Core.Helpers

  @type pair_of_strings :: nonempty_list(String.t())

  @callback usage() :: String.t() | [String.t()]
  # validates CLI arguments
  @callback validate(list(), map()) :: :ok | {:validation_failure, atom() | {atom(), String.t()}}
  @callback merge_defaults(list(), map()) :: {list(), map()}
  @callback banner(list(), map()) :: [String.t()] | String.t() | nil
  @callback run(list(), map()) :: any
  # Coerces run/2 return value into the standard command output form
  # that is then formatted, printed and returned as an exit code.
  # There is a default implementation for this callback in DefaultOutput module
  @callback output(any, map()) ::
              :ok
              | {:ok, any}
              | {:stream, Enum.t()}
              | {:error, RabbitMQ.CLI.Core.ExitCodes.exit_code(), [String.t()]}
  @optional_callbacks formatter: 0,
                      printer: 0,
                      scopes: 0,
                      usage_additional: 0,
                      usage_doc_guides: 0,
                      description: 0,
                      help_section: 0,
                      switches: 0,
                      aliases: 0,
                      # validates execution environment, e.g. file presence,
                      # whether RabbitMQ is in an expected state on a node, etc
                      validate_execution_environment: 2,
                      distribution: 1

  @callback validate_execution_environment(list(), map()) ::
              :ok | {:validation_failure, atom() | {atom(), any}}
  @callback switches() :: Keyword.t()
  @callback aliases() :: Keyword.t()

  @callback formatter() :: atom()
  @callback printer() :: atom()
  @callback scopes() :: [atom()] | nil
  @callback description() :: String.t()
  @callback help_section() :: String.t()
  @callback usage_additional() :: String.t() | [String.t()] | nonempty_list(pair_of_strings()) | [{String.t(), String.t()}]
  @callback usage_doc_guides() :: String.t() | [String.t()]
  ## Erlang distribution control
  ## :cli - default rabbitmqctl generated node name
  ## :none - disable erlang distribution
  ## {:fun, fun} - use a custom function to start distribution
  @callback distribution(map()) :: :cli | :none | {:fun, (map() -> :ok | {:error, any()})}

  def usage(cmd) do
    cmd.usage()
  end

  def scopes(cmd) do
    Helpers.apply_if_exported(cmd, :scopes, [], nil)
  end

  def description(cmd) do
    Helpers.apply_if_exported(cmd, :description, [], "")
  end

  def help_section(cmd) do
    case Helpers.apply_if_exported(cmd, :help_section, [], :other) do
      :other ->
        case Application.get_application(cmd) do
          :rabbitmqctl -> :other
          plugin -> {:plugin, plugin}
        end
      section ->
        section
    end
  end

  def usage_additional(cmd) do
    Helpers.apply_if_exported(cmd, :usage_additional, [], [])
  end

  def usage_doc_guides(cmd) do
    Helpers.apply_if_exported(cmd, :usage_doc_guides, [], [])
  end

  def formatter(cmd, default) do
    Helpers.apply_if_exported(cmd, :formatter, [], default)
  end

  def printer(cmd, default) do
    Helpers.apply_if_exported(cmd, :printer, [], default)
  end

  def switches(cmd) do
    Helpers.apply_if_exported(cmd, :switches, [], [])
  end

  def aliases(cmd) do
    Helpers.apply_if_exported(cmd, :aliases, [], [])
  end

  def validate_execution_environment(cmd, args, options) do
    Helpers.apply_if_exported(cmd,
                              :validate_execution_environment, [args, options],
                              :ok)
  end

  def distribution(cmd, options) do
    Helpers.apply_if_exported(cmd, :distribution, [options], :cli)
  end
end
