## This Source Code Form is subject to the terms of the Mozilla Public
## License, v. 2.0. If a copy of the MPL was not distributed with this
## file, You can obtain one at https://mozilla.org/MPL/2.0/.
##
## Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.

defmodule RabbitMQ.CLI.Core.Listeners do
  import Record, only: [defrecord: 3, extract: 2]
  import RabbitCommon.Records
  import RabbitMQ.CLI.Core.DataCoercion

  #
  # API
  #

  defrecord :certificate, :Certificate, extract(:Certificate, from_lib: "public_key/include/public_key.hrl")
  defrecord :tbscertificate, :TBSCertificate, extract(:TBSCertificate, from_lib: "public_key/include/public_key.hrl")
  defrecord :validity, :Validity, extract(:Validity, from_lib: "public_key/include/public_key.hrl")

  def listeners_on(listeners, target_node) do
    Enum.filter(listeners, fn listener(node: node) ->
      node == target_node
    end)
  end

  def listeners_with_certificates(listeners) do
    Enum.filter(listeners, fn listener(opts: opts) ->
      Keyword.has_key?(opts, :cacertfile) or Keyword.has_key?(opts, :certfile)
    end)
  end

  def listener_lines(listeners) do
    listeners
    |> listener_maps
    |> Enum.map(fn %{interface: interface, port: port, protocol: protocol} ->
      "Interface: #{interface}, port: #{port}, protocol: #{protocol}, purpose: #{
        protocol_label(to_atom(protocol))
      }"
    end)
  end
  def listener_lines(listeners, node) do
    listeners
    |> listener_maps
    |> Enum.map(fn %{interface: interface, port: port, protocol: protocol} ->
      "Node: #{node}, interface: #{interface}, port: #{port}, protocol: #{protocol}, purpose: #{
        protocol_label(to_atom(protocol))
      }"
    end)
  end

  def listener_map(listener) when is_map(listener) do
    listener
  end
  def listener_map(listener) do
    # Listener options are left out intentionally: they can contain deeply nested values
    # that are impossible to serialise to JSON.
    #
    # Management plugin/HTTP API had its fair share of bugs because of that
    # and now filters out a lot of options. Raw listener data can be seen in
    # rabbitmq-diagnostics status.
    listener(node: node, protocol: protocol, ip_address: interface, port: port) = listener

    %{
      node: node,
      protocol: protocol,
      interface: :inet.ntoa(interface) |> to_string |> maybe_enquote_interface,
      port: port,
      purpose: protocol_label(to_atom(protocol))
    }
  end

  def listener_maps(listeners) do
    Enum.map(listeners, &listener_map/1)
  end

  def listener_certs(listener) do
    listener(node: node, protocol: protocol, ip_address: interface, port: port, opts: opts) = listener

    %{
      node: node,
      protocol: protocol,
      interface: :inet.ntoa(interface) |> to_string |> maybe_enquote_interface,
      port: port,
      purpose: protocol_label(to_atom(protocol)),
      certfile: read_cert(Keyword.get(opts, :certfile)),
      cacertfile: read_cert(Keyword.get(opts, :cacertfile))
    }
  end

  def read_cert(nil) do
    nil
  end
  def read_cert({:pem, pem}) do
    pem
  end
  def read_cert(path) do
    case File.read(path) do
      {:ok, bin} ->
        bin
      {:error, _} = err ->
        err
    end
  end

  def listener_expiring_within(listener, seconds) do
    listener(node: node, protocol: protocol, ip_address: interface, port: port, opts: opts) = listener
    certfile = Keyword.get(opts, :certfile)
    cacertfile = Keyword.get(opts, :cacertfile)
    now = :calendar.datetime_to_gregorian_seconds(:calendar.universal_time())
    expiry_date = now + seconds
    certfile_expires_on = expired(cert_validity(read_cert(certfile)), expiry_date)
    cacertfile_expires_on = expired(cert_validity(read_cert(cacertfile)), expiry_date)
    case {certfile_expires_on, cacertfile_expires_on} do
      {[], []} ->
        false
      _ ->
        %{
          node: node,
          protocol: protocol,
          interface: interface,
          port: port,
          certfile: certfile,
          cacertfile: cacertfile,
          certfile_expires_on: certfile_expires_on,
          cacertfile_expires_on: cacertfile_expires_on
    }
    end
  end

  def expired_listener_map(%{node: node, protocol: protocol, interface: interface, port: port, certfile_expires_on: certfile_expires_on, cacertfile_expires_on: cacertfile_expires_on, certfile: certfile, cacertfile: cacertfile}) do
    %{
      node: node,
      protocol: protocol,
      interface: :inet.ntoa(interface) |> to_string |> maybe_enquote_interface,
      port: port,
      purpose: protocol_label(to_atom(protocol)),
      certfile: certfile |> to_string,
      cacertfile: cacertfile |> to_string,
      certfile_expires_on: expires_on_list(certfile_expires_on),
      cacertfile_expires_on: expires_on_list(cacertfile_expires_on)
    }
  end

  def expires_on_list({:error, _} = error) do
    [error]
  end
  def expires_on_list(expires) do
    Enum.map(expires, &expires_on/1)
  end

  def expires_on({:error, _} = error) do
    error
  end
  def expires_on(seconds) do
    {:ok, naive} = NaiveDateTime.from_erl(:calendar.gregorian_seconds_to_datetime(seconds))
    NaiveDateTime.to_string(naive)
  end

  def expired(nil, _) do
    []
  end
  def expired({:error, _} = error, _) do
    error
  end
  def expired(expires, expiry_date) do
    Enum.filter(expires, fn ({:error, _}) -> true
      (seconds) -> seconds < expiry_date end)
  end

  def cert_validity(nil) do
    nil
  end
  def cert_validity(cert) do
    dsa_entries = :public_key.pem_decode(cert)
    case dsa_entries do
      [] ->
        {:error, "The certificate file provided does not contain any PEM entry."}
      _ ->
        now = :calendar.datetime_to_gregorian_seconds(:calendar.universal_time())
        Enum.map(dsa_entries, fn ({:Certificate, _, _} = dsa_entry) ->
          certificate(tbsCertificate: tbs_certificate) = :public_key.pem_entry_decode(dsa_entry)
          tbscertificate(validity: validity) = tbs_certificate
          validity(notAfter: not_after, notBefore: not_before) = validity
          start = :pubkey_cert.time_str_2_gregorian_sec(not_before)
          case start > now do
            true ->
              {:ok, naive} = NaiveDateTime.from_erl(:calendar.gregorian_seconds_to_datetime(start))
              startdate = NaiveDateTime.to_string(naive)
              {:error, "Certificate is not yet valid. It starts on #{startdate}"}
            false ->
              :pubkey_cert.time_str_2_gregorian_sec(not_after)
          end
          ({type, _, _}) ->
            {:error, "The certificate file provided contains a #{type} entry."}
        end)
    end
  end

  def listener_rows(listeners) do
    for listener(node: node, protocol: protocol, ip_address: interface, port: port) <- listeners do
      # Listener options are left out intentionally, see above
      [
        node: node,
        protocol: protocol,
        interface: :inet.ntoa(interface) |> to_string |> maybe_enquote_interface,
        port: port,
        purpose: protocol_label(to_atom(protocol))
      ]
    end
  end

  def protocol_label(:amqp),       do: "AMQP 0-9-1 and AMQP 1.0"
  def protocol_label(:'amqp/ssl'), do: "AMQP 0-9-1 and AMQP 1.0 over TLS"
  def protocol_label(:mqtt),       do: "MQTT"
  def protocol_label(:'mqtt/ssl'), do: "MQTT over TLS"
  def protocol_label(:stomp),       do: "STOMP"
  def protocol_label(:'stomp/ssl'), do: "STOMP over TLS"
  def protocol_label(:http),  do: "HTTP API"
  def protocol_label(:https), do: "HTTP API over TLS (HTTPS)"
  def protocol_label(:"http/web-mqtt"), do: "MQTT over WebSockets"
  def protocol_label(:"https/web-mqtt"), do: "MQTT over WebSockets and TLS (HTTPS)"
  def protocol_label(:"http/web-stomp"), do: "STOMP over WebSockets"
  def protocol_label(:"https/web-stomp"), do: "STOMP over WebSockets and TLS (HTTPS)"
  def protocol_label(:"http/prometheus"), do: "Prometheus exporter API over HTTP"
  def protocol_label(:"https/prometheus"), do: "Prometheus exporter API over TLS (HTTPS)"
  def protocol_label(:clustering), do: "inter-node and CLI tool communication"
  def protocol_label(other), do: to_string(other)

  def normalize_protocol(proto) do
    val = proto |> to_string |> String.downcase()

    case val do
      "amqp091" -> "amqp"
      "amqp0.9.1" -> "amqp"
      "amqp0-9-1" -> "amqp"
      "amqp0_9_1" -> "amqp"
      "amqp10" -> "amqp"
      "amqp1.0" -> "amqp"
      "amqp1-0" -> "amqp"
      "amqp1_0" -> "amqp"
      "amqps" -> "amqp/ssl"
      "mqtt3.1" -> "mqtt"
      "mqtt3.1.1" -> "mqtt"
      "mqtt31" -> "mqtt"
      "mqtt311" -> "mqtt"
      "mqtt3_1" -> "mqtt"
      "mqtt3_1_1" -> "mqtt"
      "mqtts"     -> "mqtt/ssl"
      "mqtt+tls"  -> "mqtt/ssl"
      "mqtt+ssl"  -> "mqtt/ssl"
      "stomp1.0" -> "stomp"
      "stomp1.1" -> "stomp"
      "stomp1.2" -> "stomp"
      "stomp10" -> "stomp"
      "stomp11" -> "stomp"
      "stomp12" -> "stomp"
      "stomp1_0" -> "stomp"
      "stomp1_1" -> "stomp"
      "stomp1_2" -> "stomp"
      "stomps"    -> "stomp/ssl"
      "stomp+tls" -> "stomp/ssl"
      "stomp+ssl" -> "stomp/ssl"
      "https" -> "https"
      "http1" -> "http"
      "http1.1" -> "http"
      "http_api" -> "http"
      "management" -> "http"
      "management_ui" -> "http"
      "ui" -> "http"
      "cli" -> "clustering"
      "distribution" -> "clustering"
      "webmqtt" -> "http/web-mqtt"
      "web-mqtt" -> "http/web-mqtt"
      "web_mqtt" -> "http/web-mqtt"
      "webmqtt/tls" -> "https/web-mqtt"
      "web-mqtt/tls" -> "https/web-mqtt"
      "webmqtt/ssl" -> "https/web-mqtt"
      "web-mqtt/ssl" -> "https/web-mqtt"
      "webmqtt+tls" -> "https/web-mqtt"
      "web-mqtt+tls" -> "https/web-mqtt"
      "webmqtt+ssl" -> "https/web-mqtt"
      "web-mqtt+ssl" -> "https/web-mqtt"
      "webstomp" -> "http/web-stomp"
      "web-stomp" -> "http/web-stomp"
      "web_stomp" -> "http/web-stomp"
      "webstomp/tls" -> "https/web-stomp"
      "web-stomp/tls" -> "https/web-stomp"
      "webstomp/ssl" -> "https/web-stomp"
      "web-stomp/ssl" -> "https/web-stomp"
      "webstomp+tls" -> "https/web-stomp"
      "web-stomp+tls" -> "https/web-stomp"
      "webstomp+ssl" -> "https/web-stomp"
      "web-stomp+ssl" -> "https/web-stomp"
      _ -> val
    end
  end

  #
  # Implementation
  #

  defp maybe_enquote_interface(value) do
    # This simplistic way of distinguishing IPv6 addresses,
    # networks address ranges, etc actually works better
    # for the kind of values we can get here than :inet functions. MK.
    regex = Regex.recompile!(~r/:/)
    case value =~ regex do
      true -> "[#{value}]"
      false -> value
    end
  end
end
