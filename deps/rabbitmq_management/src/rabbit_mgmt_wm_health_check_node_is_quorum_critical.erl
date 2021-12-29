%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.
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
            rabbit_mgmt_util:reply([{status, ok},
                                    {reason, <<"single node cluster">>}], ReqData, Context);
        false ->
            case rabbit_quorum_queue:list_with_minimum_quorum_for_cli() of
                [] ->
                    rabbit_mgmt_util:reply([{status, ok}], ReqData, Context);
                Qs when length(Qs) > 0 ->
                    Msg = <<"There are quorum queues that would lose their quorum if the target node is shut down">>,
                    failure(Msg, Qs, ReqData, Context)
            end
    end.

failure(Message, Qs, ReqData, Context) ->
    {Response, ReqData1, Context1} = rabbit_mgmt_util:reply([{status, failed},
                                                             {reason, Message},
                                                             {queues, Qs}],
                                                            ReqData, Context),
    {stop, cowboy_req:reply(503, #{}, Response, ReqData1), Context1}.

is_authorized(ReqData, Context) ->
    rabbit_mgmt_util:is_authorized(ReqData, Context).
