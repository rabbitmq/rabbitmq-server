%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_mgmt_wm_auth_attempts).

-export([init/2, to_json/2, content_types_provided/2, allowed_methods/2, is_authorized/2,
         delete_resource/2, resource_exists/2]).
-export([variances/2]).

-include_lib("rabbitmq_management_agent/include/rabbit_mgmt_records.hrl").
%%--------------------------------------------------------------------
init(Req, [Mode]) ->
    {cowboy_rest, rabbit_mgmt_headers:set_common_permission_headers(Req, ?MODULE),
     {Mode, #context{}}}.

variances(Req, Context) ->
    {[<<"accept-encoding">>, <<"origin">>], Req, Context}.

content_types_provided(ReqData, Context) ->
   {rabbit_mgmt_util:responder_map(to_json), ReqData, Context}.

allowed_methods(ReqData, Context) ->
    {[<<"HEAD">>, <<"GET">>, <<"DELETE">>, <<"OPTIONS">>], ReqData, Context}.

resource_exists(ReqData, Context) ->
    {rabbit_mgmt_nodes:node_exists(ReqData), ReqData, Context}.

to_json(ReqData, {Mode, Context}) ->
    rabbit_mgmt_util:reply(augment(Mode, ReqData), ReqData, Context).

is_authorized(ReqData, {Mode, Context}) ->
    AuthFun = case cowboy_req:method(ReqData) of
                  <<"DELETE">> -> fun rabbit_mgmt_util:is_authorized_admin/2;
                  _            -> fun rabbit_mgmt_util:is_authorized_monitor/2
              end,
    {Res, Req2, Context2} = AuthFun(ReqData, Context),
    {Res, Req2, {Mode, Context2}}.

delete_resource(ReqData, Context) ->
    case rabbit_mgmt_nodes:node_name_from_req(ReqData) of
        {error, _} ->
            {false, ReqData, Context};
        {ok, Node} ->
            case rpc:call(Node, rabbit_core_metrics, reset_auth_attempt_metrics, [], infinity) of
                {badrpc, _} -> {false, ReqData, Context};
                ok          -> {true, ReqData, Context}
            end
    end.
%%--------------------------------------------------------------------

augment(Mode, ReqData) ->
    case rabbit_mgmt_nodes:node_name_from_req(ReqData) of
        {error, _} ->
            not_found;
        {ok, Node} ->
            Fun = case Mode of
                      all -> get_auth_attempts;
                      by_source -> get_auth_attempts_by_source
                  end,
            case rpc:call(Node, rabbit_core_metrics, Fun, [], infinity) of
                {badrpc, _} -> not_available;
                Result      -> Result
            end
    end.
