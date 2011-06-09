%%   The contents of this file are subject to the Mozilla Public License
%%   Version 1.1 (the "License"); you may not use this file except in
%%   compliance with the License. You may obtain a copy of the License at
%%   http://www.mozilla.org/MPL/
%%
%%   Software distributed under the License is distributed on an "AS IS"
%%   basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
%%   License for the specific language governing rights and limitations
%%   under the License.
%%
%%   The Original Code is RabbitMQ Management Plugin.
%%
%%   The Initial Developer of the Original Code is VMware, Inc.
%%   Copyright (c) 2007-2010 VMware, Inc.  All rights reserved.
-module(rabbit_mgmt_wm_overview).

-export([init/1, to_json/2, content_types_provided/2, is_authorized/2]).

-include("rabbit_mgmt.hrl").
-include_lib("webmachine/include/webmachine.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").

%%--------------------------------------------------------------------

init(_Config) -> {ok, #context{}}.

content_types_provided(ReqData, Context) ->
   {[{"application/json", to_json}], ReqData, Context}.

to_json(ReqData, Context = #context{user = User = #user{tags = Tags}}) ->
    {ok, StatsLevel} = application:get_env(rabbit, collect_statistics),
    %% NB: node and stats level duplicate what's in /nodes but we want
    %% to (a) know which node we're talking to and (b) use the stats
    %% level to switch features on / off in the UI.
    Overview0 = [{management_version, version()},
                 {statistics_level,   StatsLevel}],
    Overview =
        case rabbit_mgmt_util:is_admin(Tags) of
            true ->
                Listeners = [rabbit_mgmt_format:listener(L)
                             || L <- rabbit_networking:active_listeners()],
                Overview0 ++
                    rabbit_mgmt_db:get_overview() ++
                    [{node,               node()},
                     {statistics_db_node, stats_db_node()},
                     {listeners,          Listeners}];
            _ ->
                Overview0 ++
                    rabbit_mgmt_db:get_overview(User)
        end,
    rabbit_mgmt_util:reply(Overview, ReqData, Context).

is_authorized(ReqData, Context) ->
    rabbit_mgmt_util:is_authorized(ReqData, Context).

%%--------------------------------------------------------------------

stats_db_node() ->
    case global:whereis_name(rabbit_mgmt_db) of
        undefined -> not_running;
        Pid       -> node(Pid)
    end.

version() ->
    [Vsn] = [V || {A, _D, V} <- application:loaded_applications(),
            A =:= rabbitmq_management],
    list_to_binary(Vsn).

