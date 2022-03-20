%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

%% An HTTP API counterpart of 'rabbitmq-diagnostics check_port_listener'
-module(rabbit_mgmt_wm_health_check_port_listener).

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
    {case port(ReqData) of
         none -> false;
         _ -> true
     end, ReqData, Context}.

to_json(ReqData, Context) ->
    try
        Port = binary_to_integer(port(ReqData)),
        Listeners = rabbit_networking:active_listeners(),
        Local = [L || #listener{node = N} = L <- Listeners, N == node()],
        PortListeners = [L || #listener{port = P} = L <- Local, P == Port],
        case PortListeners of
            [] ->
                Msg = <<"No active listener">>,
                failure(Msg, Port, [P || #listener{port = P} <- Local], ReqData, Context);
            _ ->
                rabbit_mgmt_util:reply([{status, ok},
                                        {port, Port}], ReqData, Context)
        end
    catch
        error:badarg ->
            rabbit_mgmt_util:bad_request(<<"Invalid port">>, ReqData, Context)
    end.

failure(Message, Missing, Ports, ReqData, Context) ->
    {Response, ReqData1, Context1} = rabbit_mgmt_util:reply([{status, failed},
                                                             {reason, Message},
                                                             {missing, Missing},
                                                             {ports, Ports}],
                                                            ReqData, Context),
    {stop, cowboy_req:reply(503, #{}, Response, ReqData1), Context1}.

is_authorized(ReqData, Context) ->
    rabbit_mgmt_util:is_authorized(ReqData, Context).

port(ReqData) ->
    rabbit_mgmt_util:id(port, ReqData).
