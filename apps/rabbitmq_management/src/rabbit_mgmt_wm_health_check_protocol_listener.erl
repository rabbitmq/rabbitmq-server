%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.
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
    {case protocol(ReqData) of
         none -> false;
         _ -> true
     end, ReqData, Context}.

to_json(ReqData, Context) ->
    Protocol = normalize_protocol(protocol(ReqData)),
    Listeners = rabbit_networking:active_listeners(),
    Local = [L || #listener{node = N} = L <- Listeners, N == node()],
    ProtoListeners = [L || #listener{protocol = P} = L <- Local, atom_to_list(P) == Protocol],
    case ProtoListeners of
        [] ->
            Msg = <<"No active listener">>,
            failure(Msg, Protocol, [P || #listener{protocol = P} <- Local], ReqData, Context);
        _ ->
            rabbit_mgmt_util:reply([{status, ok},
                                    {protocol, list_to_binary(Protocol)}], ReqData, Context)
    end.

failure(Message, Missing, Protocols, ReqData, Context) ->
    {Response, ReqData1, Context1} = rabbit_mgmt_util:reply([{status, failed},
                                                             {reason, Message},
                                                             {missing, list_to_binary(Missing)},
                                                             {protocols, Protocols}],
                                                            ReqData, Context),
    {stop, cowboy_req:reply(503, #{}, Response, ReqData1), Context1}.

is_authorized(ReqData, Context) ->
    rabbit_mgmt_util:is_authorized(ReqData, Context).

protocol(ReqData) ->
    rabbit_mgmt_util:id(protocol, ReqData).

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
