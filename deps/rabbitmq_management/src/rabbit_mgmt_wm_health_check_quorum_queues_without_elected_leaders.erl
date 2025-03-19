%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

%% An HTTP API counterpart of 'rabbitmq-diagnostics check_for_quorum_queues_without_an_elected_leader'
-module(rabbit_mgmt_wm_health_check_quorum_queues_without_elected_leaders).

-export([init/2, to_json/2, content_types_provided/2, is_authorized/2]).
-export([resource_exists/2]).
-export([variances/2]).

-include_lib("rabbitmq_management_agent/include/rabbit_mgmt_records.hrl").

-define(DEFAULT_PATTERN, <<".*">>).

%%--------------------------------------------------------------------

init(Req, _State) ->
    {cowboy_rest, rabbit_mgmt_headers:set_common_permission_headers(Req, ?MODULE), #context{}}.

variances(Req, Context) ->
    {[<<"accept-encoding">>, <<"origin">>], Req, Context}.

content_types_provided(ReqData, Context) ->
    {rabbit_mgmt_util:responder_map(to_json), ReqData, Context}.

resource_exists(ReqData, Context) ->
    Result = case {vhost(ReqData), pattern(ReqData)} of
                 {none, _} -> false;
                 {_, none} -> false;
                 _         -> true
             end,
    {Result, ReqData, Context}.

to_json(ReqData, Context) ->
    case rabbit_quorum_queue:leader_health_check(pattern(ReqData), vhost(ReqData)) of
        [] ->
            rabbit_mgmt_util:reply(#{status => ok}, ReqData, Context);
        Qs when length(Qs) > 0 ->
            Msg = <<"Detected quorum queues without an elected leader">>,
            failure(Msg, Qs, ReqData, Context)
    end.

failure(Message, Qs, ReqData, Context) ->
    Body = #{status => failed,
             reason => Message,
             queues => Qs},
    {Response, ReqData1, Context1} = rabbit_mgmt_util:reply(Body, ReqData, Context),
    {stop, cowboy_req:reply(503, #{}, Response, ReqData1), Context1}.

is_authorized(ReqData, Context) ->
    rabbit_mgmt_util:is_authorized(ReqData, Context).

%%
%% Implementation
%%

vhost(ReqData) ->
    rabbit_mgmt_util:id(vhost, ReqData).

pattern(ReqData) ->
    case rabbit_mgmt_util:id(pattern, ReqData) of
        none  -> ?DEFAULT_PATTERN;
        Other -> Other
    end.
