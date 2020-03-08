%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License at
%% https://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
%% License for the specific language governing rights and limitations
%% under the License.
%%
%% The Original Code is RabbitMQ Management Plugin.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2007-2020 Pivotal Software, Inc.  All rights reserved.
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
            rabbit_amqqueue:rebalance(all, ".*", ".*")
        end),
        {true, Req, State}
    catch
        {error, Reason} ->
            rabbit_mgmt_util:bad_request(iolist_to_binary(Reason), Req, State)
    end.

is_authorized(Req0, {Mode, Context0}) ->
    {Res, Req1, Context1} = rabbit_mgmt_util:is_authorized_admin(Req0, Context0),
    {Res, Req1, {Mode, Context1}}.
