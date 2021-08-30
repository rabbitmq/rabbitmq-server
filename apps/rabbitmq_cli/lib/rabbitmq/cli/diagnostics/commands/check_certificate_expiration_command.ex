## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.

defmodule RabbitMQ.CLI.Diagnostics.Commands.CheckCertificateExpirationCommand do
  alias RabbitMQ.CLI.Core.DocGuide
  alias RabbitMQ.CLI.TimeUnit, as: TU
  @behaviour RabbitMQ.CLI.CommandBehaviour

  import RabbitMQ.CLI.Core.Listeners

  def switches(), do: [unit: :string, within: :integer]

  def merge_defaults(args, opts) do
    {args, Map.merge(%{unit: "weeks", within: 4}, opts)}
  end

  def validate(args, _) when length(args) > 0 do
    {:validation_failure, :too_many_args}
  end
  def validate(_, %{unit: unit}) do
    case TU.known_unit?(unit) do
      true ->
        :ok

      false ->
        {:validation_failure, "unit '#{unit}' is not supported. Please use one of: days, weeks, months, years"}
    end
  end
  def validate(_, _), do: :ok

  def run([], %{node: node_name, unit: unit, within: within, timeout: timeout}) do
    case :rabbit_misc.rpc_call(node_name, :rabbit_networking, :active_listeners, [], timeout) do
      {:error, _} = err ->
        err

      {:error, _, _} = err ->
        err

      {:badrpc, _} = err ->
        err

      xs when is_list(xs) ->
        listeners = listeners_on(xs, node_name)
        seconds = TU.convert(within, unit)
        Enum.reduce(listeners, [], fn (listener, acc) -> case listener_expiring_within(listener, seconds) do
                                                           false -> acc
                                                           expiring -> [expiring | acc]
                                                         end
        end)
    end
  end

  def output([], %{formatter: "json"}) do
    {:ok, %{"result" => "ok"}}
  end

  def output([], %{unit: unit, within: within}) do
    unit_label = unit_label(within, unit)
    {:ok, "No certificates are expiring within #{within} #{unit_label}."}
  end

  def output(listeners, %{formatter: "json"}) do
    {:error, :check_failed, %{"result" => "error", "expired" => Enum.map(listeners, &expired_listener_map/1)}}
  end

  def output(listeners, %{}) do
    {:error, :check_failed, Enum.map(listeners, &expired_listener_map/1)}
  end

  def unit_label(1, unit) do
    unit |> String.slice(0..-2)
  end
  def unit_label(_within, unit) do
    unit
  end

  def usage, do: "check_certificate_expiration [--within <period>] [--unit <unit>]"

  def usage_additional() do
    [
      ["<period>", "period of time to check. Default is four (weeks)."],
      ["<unit>", "time unit for the period, can be days, weeks, months, years. Default is weeks."],
    ]
  end

  def usage_doc_guides() do
    [
      DocGuide.tls(),
      DocGuide.networking()
    ]
  end

  def help_section(), do: :observability_and_health_checks

  def description(), do: "Checks the expiration date on the certificates for every listener configured to use TLS"

  def banner(_, %{node: node_name}), do: "Checking certificate expiration on node #{node_name} ..."
end
