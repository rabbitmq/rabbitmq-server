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
## Copyright (c) 2007-2019 Pivotal Software, Inc.  All rights reserved.

defmodule RabbitMQ.CLI.Diagnostics.Commands.CipherSuitesCommand do
  @behaviour RabbitMQ.CLI.CommandBehaviour
  use RabbitMQ.CLI.DefaultOutput

  def merge_defaults(args, %{erlang_format: true} = opts) do
    {args, opts}
  end

  def merge_defaults(args, opts) do
    {args, Map.merge(%{openssl_format: true, erlang_format: false}, opts)}
  end

  def switches(), do: [timeout: :integer, openssl_format: :boolean, erlang_format: :boolean]
  def aliases(), do: [t: :timeout]

  def validate(_, %{openssl_format: true, erlang_format: true}) do
    {:validation_failure, {:bad_argument, "Cannot use both formats together"}}
  end

  def validate(_, %{openssl_format: false, erlang_format: false}) do
    {:validation_failure,
     {:bad_argument, "Either OpenSSL or Erlang term format must be selected"}}
  end

  def validate(args, _) when length(args) > 0 do
    {:validation_failure, :too_many_args}
  end

  def validate(_, _), do: :ok

  def run([], %{node: node_name, timeout: timeout, openssl_format: true} = _opts) do
    :rabbit_misc.rpc_call(node_name, :ssl, :cipher_suites, [:openssl], timeout)
  end

  def run([], %{node: node_name, timeout: timeout, openssl_format: false} = _opts) do
    :rabbit_misc.rpc_call(node_name, :ssl, :cipher_suites, [], timeout)
  end

  def run([], %{node: node_name, timeout: timeout, erlang_format: true} = _opts) do
    :rabbit_misc.rpc_call(node_name, :ssl, :cipher_suites, [], timeout)
  end

  def help_section(), do: :observability_and_health_checks

  def usage, do: "cipher_suites [--openssl-format] [--erlang-format]"

  def banner([], %{openssl_format: true}),
    do: "Listing available cipher suites in the OpenSSL format"

  def banner([], %{erlang_format: true}),
    do: "Listing available cipher suites in the Erlang term format"

  defmodule Formatter do
    alias RabbitMQ.CLI.Formatters.FormatterHelpers

    @behaviour RabbitMQ.CLI.FormatterBehaviour

    def format_output(item, %{openssl_format: false} = _opts) do
      to_string(:io_lib.format("~p", [item]))
    end

    def format_output(item, %{erlang_format: true} = _opts) do
      to_string(:io_lib.format("~p", [item]))
    end

    def format_output(item, %{openssl_format: true} = opts) do
      RabbitMQ.CLI.Formatters.String.format_output(item, opts)
    end

    def format_stream(stream, %{openssl_format: false} = opts) do
      comma_separated(stream, opts)
    end

    def format_stream(stream, %{erlang_format: true} = opts) do
      comma_separated(stream, opts)
    end

    def format_stream(stream, %{openssl_format: true} = opts) do
      Stream.map(
        stream,
        FormatterHelpers.without_errors_1(fn el ->
          format_output(el, opts)
        end)
      )
    end

    defp comma_separated(stream, opts) do
      elements =
        Stream.scan(
          stream,
          :empty,
          FormatterHelpers.without_errors_2(fn element, previous ->
            separator =
              case previous do
                :empty -> ""
                _ -> ","
              end

            format_element(element, separator, opts)
          end)
        )

      Stream.concat([["["], elements, ["]"]])
    end

    defp format_element(val, separator, opts) do
      separator <> format_output(val, opts)
    end
  end

  def formatter(), do: Formatter
end
