%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

%% An HTTP API counterpart of 'rabbitmq-diagnostics check_if_node_is_quorum_critical'
-module(rabbit_mgmt_wm_health_check_node_is_quorum_critical).

-export([init/2, to_json/2, content_types_provided/2, is_authorized/2]).
-export([resource_exists/2]).
-export([variances/2]).

-include_lib("rabbitmq_management_agent/include/rabbit_mgmt_records.hrl").

%%--------------------------------------------------------------------

init(Req, _State) ->
    {cowboy_rest, rabbit_mgmt_headers:set_common_permission_headers(Req, ?MODULE), #context{}}.

variances(Req, Context) ->
    {[<<"accept-encoding">>, <<"origin">>], Req, Context}.

content_types_provided(ReqData, Context) ->
   {rabbit_mgmt_util:responder_map(to_json), ReqData, Context}.

resource_exists(ReqData, Context) ->
    {true, ReqData, Context}.

to_json(ReqData, Context) ->
    case rabbit_nodes:is_single_node_cluster() of
        true ->
            rabbit_mgmt_util:reply(#{status => ok,
                                     reason => <<"single node cluster">>}, ReqData, Context);
        false ->
            case rabbit_upgrade_preparation:list_with_minimum_quorum_for_cli() of
                [] ->
                    rabbit_mgmt_util:reply(#{status => ok}, ReqData, Context);
                Qs when length(Qs) > 0 ->
                    reply_for_quorum_critical_queues(
                      filter_vhost(Qs, ReqData, Context), ReqData, Context)
            end
    end.

reply_for_quorum_critical_queues([], ReqData, Context) ->
    rabbit_mgmt_util:reply(#{status => ok}, ReqData, Context);
reply_for_quorum_critical_queues(Qs, ReqData, Context) ->
    Msg = <<"There are quorum queues that would lose their "
            "quorum if the target node is shut down">>,
    failure(Msg, Qs, ReqData, Context).

failure(Message, Qs, ReqData, Context) ->
    Body = #{status => failed,
             reason => Message,
             queues => Qs},
    {Response, ReqData1, Context1} = rabbit_mgmt_util:reply(Body, ReqData, Context),
    {stop, cowboy_req:reply(503, #{}, Response, ReqData1), Context1}.

is_authorized(ReqData, Context) ->
    rabbit_mgmt_util:is_authorized(ReqData, Context).

%% Qs are #{binary() => any()} maps keyed by <<"virtual_host">>, not the
%% atom-keyed proplists filter_vhost/3 expects, so tag and untag around it.
%% Critical components (rabbit_stream_coordinator, rabbitmq_metadata) are
%% cluster-wide, not vhost-scoped, and must pass through unfiltered rather
%% than be dropped for everyone. They are marked with `type => process`
%% (rabbit_upgrade_preparation:list_with_minimum_quorum_for_cli/0); vhost
%% names are arbitrary binaries, so matching on the display-only
%% virtual_host value of "(not applicable)" could be spoofed by a real
%% vhost of that name.
filter_vhost(Qs, ReqData, Context) ->
    IsVhostScoped = fun(Q) -> maps:get(<<"type">>, Q) =/= process end,
    {VhostScoped, NotVhostScoped} = lists:partition(IsVhostScoped, Qs),
    Tagged = [maps:put(vhost, maps:get(<<"virtual_host">>, Q), Q)
              || Q <- VhostScoped],
    Filtered = rabbit_mgmt_util:filter_vhost(Tagged, ReqData, Context),
    NotVhostScoped ++ [maps:remove(vhost, Q) || Q <- Filtered].
