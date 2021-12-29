%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_mgmt_wm_rebalance_queues).

-export([init/2, service_available/2, resource_exists/2,
         content_types_provided/2, content_types_accepted/2,
         is_authorized/2, allowed_methods/2, accept_content/2]).
-export([variances/2]).

-include_lib("rabbitmq_management_agent/include/rabbit_mgmt_records.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").

%%--------------------------------------------------------------------

init(Req, [Mode]) ->
    Headers = rabbit_mgmt_headers:set_common_permission_headers(Req, ?MODULE),
    {cowboy_rest, Headers, {Mode, #context{}}}.

service_available(Req, {{queues, all}, _Context}=State) ->
    {true, Req, State};
service_available(Req, State) ->
    {false, Req, State}.

variances(Req, State) ->
    {[<<"accept-encoding">>, <<"origin">>], Req, State}.

content_types_provided(Req, State) ->
    {[{{<<"text">>, <<"plain">>, '*'}, undefined}], Req, State}.

content_types_accepted(Req, State) ->
   {[{'*', accept_content}], Req, State}.

allowed_methods(Req, State) ->
    {[<<"POST">>, <<"OPTIONS">>], Req, State}.

resource_exists(Req, State) ->
    {true, Req, State}.

accept_content(Req, {_Mode, #context{user = #user{username = Username}}}=State) ->
    try
        rabbit_log:info("User '~s' has initiated a queue rebalance", [Username]),
        spawn(fun() ->
            rabbit_amqqueue:rebalance(all, <<".*">>, <<".*">>)
        end),
        {true, Req, State}
    catch
        {error, Reason} ->
            rabbit_mgmt_util:bad_request(iolist_to_binary(Reason), Req, State)
    end.

is_authorized(Req0, {Mode, Context0}) ->
    {Res, Req1, Context1} = rabbit_mgmt_util:is_authorized_admin(Req0, Context0),
    {Res, Req1, {Mode, Context1}}.
