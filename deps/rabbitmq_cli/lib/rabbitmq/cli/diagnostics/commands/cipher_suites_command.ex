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

defmodule RabbitMQ.CLI.Diagnostics.Commands.CipherSuitesCommand do
  @behaviour RabbitMQ.CLI.CommandBehaviour
  use RabbitMQ.CLI.DefaultOutput

  def merge_defaults(args, opts) do
    format_opts = case opts do
      %{openssl_format: true} -> %{format: "openssl", all: false};
      %{}                     -> %{format: "erlang", all: false}
    end
    {args, Map.merge(format_opts,
                     Map.delete(case_insensitive_format(opts), :openssl_format))}
  end

  defp case_insensitive_format(%{format: format} = opts) do
    %{ opts | format: String.downcase(format) }
  end
  defp case_insensitive_format(opts), do: opts

  def switches(), do: [timeout: :integer,
                       openssl_format: :boolean,
                       format: :string,
                       all: :boolean]
  def aliases(), do: [t: :timeout]

  def validate(_, %{format: format})
      when format != "openssl" and format != "erlang" and format != "map" do
    {:validation_failure, {:bad_argument, "Format must be one of: openssl, erlang or map"}}
  end
  def validate(args, _) when length(args) > 0 do
    {:validation_failure, :too_many_args}
  end

  def validate(_, _), do: :ok

  def run([], %{node: node_name, timeout: timeout, format: format} = opts) do
    {mod, function} = case format do
      "openssl" -> {:rabbit_ssl, :cipher_suites_openssl};
      "erlang"  -> {:rabbit_ssl, :cipher_suites_erlang};
      "map"     -> {:rabbit_ssl, :cipher_suites}
    end
    args = case opts do
      %{all: true} -> [:all];
      %{}          -> [:default]
    end
    :rabbit_misc.rpc_call(node_name, mod, function, args, timeout)
  end

  def banner([], %{format: "openssl"}),  do: "Listing available cipher suites in OpenSSL format"
  def banner([], %{format: "erlang"}), do: "Listing available cipher suites in Erlang term format"
  def banner([], %{format: "map"}), do: "Listing available cipher suites in map format"

  def help_section(), do: :observability_and_health_checks

  def description(), do: "Lists cipher suites enabled by default. To list all available cipher suites, add the --all argument."

  def usage, do: "cipher_suites [--format (openssl | erlang | map)] [--all]"

  def usage_additional() do
    [
      ["--format", "output format to use: openssl, erlang or map"],
      ["--all", "list all available suites"],
      ["--openssl-format", "use OpenSSL format. Deprecated: use --format=openssl instead"]
    ]
  end

  defmodule Formatter do
    alias RabbitMQ.CLI.Formatters.FormatterHelpers

    @behaviour RabbitMQ.CLI.FormatterBehaviour

    def format_output(item, %{format: "erlang"}) do
      to_string(:io_lib.format("~p", [item]))
    end

    def format_output(item, %{format: "map"}) do
      to_string(:io_lib.format("~p", [item]))
    end

    def format_output(item, %{format: "openssl"} = opts) do
      RabbitMQ.CLI.Formatters.String.format_output(item, opts)
    end

    def format_stream(stream, %{format: "erlang"} = opts) do
      comma_separated(stream, opts)
    end

    def format_stream(stream, %{format: "map"} = opts) do
      comma_separated(stream, opts)
    end

    def format_stream(stream, %{format: "openssl"} = opts) do
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
