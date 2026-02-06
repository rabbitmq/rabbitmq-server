%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

%% An HTTP API counterpart of 'rabbitmq-diagnostics check_protocol_listener'
-module(rabbit_mgmt_wm_health_check_protocol_listener).

-export([init/2, to_json/2, content_types_provided/2, is_authorized/2]).
-export([resource_exists/2]).
-export([variances/2]).

-include_lib("rabbitmq_management_agent/include/rabbit_mgmt_records.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").

%%--------------------------------------------------------------------

init(Req, _State) ->
    {cowboy_rest, rabbit_mgmt_headers:set_common_permission_headers(Req, ?MODULE), #context{}}.

variances(Req, Context) ->
    {[<<"accept-encoding">>, <<"origin">>], Req, Context}.

content_types_provided(ReqData, Context) ->
   {rabbit_mgmt_util:responder_map(to_json), ReqData, Context}.

resource_exists(ReqData, Context) ->
    {case protocols(ReqData) of
         none -> false;
         _ -> true
     end, ReqData, Context}.

to_json(ReqData, Context) ->
    Protocols = string:split(protocols(ReqData), ",", all),
    RequestedProtocols = sets:from_list(
                           [normalize_protocol(P) || P <- Protocols],
                           [{version, 2}]),
    Listeners = rabbit_networking:node_listeners(node()),
    ActiveProtocols = sets:from_list(
                        [atom_to_list(P) || #listener{protocol = P} <- Listeners],
                        [{version, 2}]),
    MissingProtocols = sets:to_list(sets:subtract(RequestedProtocols, ActiveProtocols)),
    case MissingProtocols of
        [] ->
            Body = #{status    => ok,
                     protocols => [list_to_binary(P) || P <- sets:to_list(ActiveProtocols)]},
            rabbit_mgmt_util:reply(Body, ReqData, Context);
        _ ->
            Msg = <<"No active listener">>,
            failure(Msg, MissingProtocols, sets:to_list(ActiveProtocols), ReqData, Context)
    end.

failure(Message, Missing, Protocols, ReqData, Context) ->
    Body = #{
        status    => failed,
        reason    => Message,
        missing   => [list_to_binary(P) || P <- Missing],
        protocols => [list_to_binary(P) || P <- Protocols]
    },
    {Response, ReqData1, Context1} = rabbit_mgmt_util:reply(Body, ReqData, Context),
    {stop, cowboy_req:reply(503, #{}, Response, ReqData1), Context1}.

is_authorized(ReqData, Context) ->
    rabbit_mgmt_util:is_authorized(ReqData, Context).

protocols(ReqData) ->
    rabbit_mgmt_util:id(protocols, ReqData).

normalize_protocol(Protocol) ->
    case string:lowercase(binary_to_list(Protocol)) of
        "amqp091" -> "amqp";
        "amqp0.9.1" -> "amqp";
        "amqp0-9-1" -> "amqp";
        "amqp0_9_1" -> "amqp";
        "amqp10" -> "amqp";
        "amqp1.0" -> "amqp";
        "amqp1-0" -> "amqp";
        "amqp1_0" -> "amqp";
        "amqps" -> "amqp/ssl";
        "mqtt3.1" -> "mqtt";
        "mqtt3.1.1" -> "mqtt";
        "mqtt31" -> "mqtt";
        "mqtt311" -> "mqtt";
        "mqtt3_1" -> "mqtt";
        "mqtt3_1_1" -> "mqtt";
        "mqtts"     -> "mqtt/ssl";
        "mqtt+tls"  -> "mqtt/ssl";
        "mqtt+ssl"  -> "mqtt/ssl";
        "stomp1.0" -> "stomp";
        "stomp1.1" -> "stomp";
        "stomp1.2" -> "stomp";
        "stomp10" -> "stomp";
        "stomp11" -> "stomp";
        "stomp12" -> "stomp";
        "stomp1_0" -> "stomp";
        "stomp1_1" -> "stomp";
        "stomp1_2" -> "stomp";
        "stomps"    -> "stomp/ssl";
        "stomp+tls" -> "stomp/ssl";
        "stomp+ssl" -> "stomp/ssl";
        "https" -> "https";
        "http1" -> "http";
        "http1.1" -> "http";
        "http_api" -> "http";
        "management" -> "http";
        "management_ui" -> "http";
        "ui" -> "http";
        "cli" -> "clustering";
        "distribution" -> "clustering";
        "cli/ssl" -> "clustering/ssl";
        "cli/tls" -> "clustering/ssl";
        "distribution/ssl" -> "clustering/ssl";
        "distribution/tls" -> "clustering/ssl";
        "webmqtt" -> "http/web-mqtt";
        "web-mqtt" -> "http/web-mqtt";
        "web_mqtt" -> "http/web-mqtt";
        "webmqtt/tls" -> "https/web-mqtt";
        "web-mqtt/tls" -> "https/web-mqtt";
        "webmqtt/ssl" -> "https/web-mqtt";
        "web-mqtt/ssl" -> "https/web-mqtt";
        "webmqtt+tls" -> "https/web-mqtt";
        "web-mqtt+tls" -> "https/web-mqtt";
        "webmqtt+ssl" -> "https/web-mqtt";
        "web-mqtt+ssl" -> "https/web-mqtt";
        "webstomp" -> "http/web-stomp";
        "web-stomp" -> "http/web-stomp";
        "web_stomp" -> "http/web-stomp";
        "webstomp/tls" -> "https/web-stomp";
        "web-stomp/tls" -> "https/web-stomp";
        "webstomp/ssl" -> "https/web-stomp";
        "web-stomp/ssl" -> "https/web-stomp";
        "webstomp+tls" -> "https/web-stomp";
        "web-stomp+tls" -> "https/web-stomp";
        "webstomp+ssl" -> "https/web-stomp";
        "web-stomp+ssl" -> "https/web-stomp";
        Any -> Any
    end.
