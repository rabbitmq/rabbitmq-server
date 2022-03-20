%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_mgmt_wm_reset).

-export([init/2, is_authorized/2, resource_exists/2,
         allowed_methods/2, delete_resource/2]).
-export([variances/2]).

-include_lib("rabbitmq_management_agent/include/rabbit_mgmt_records.hrl").

%%--------------------------------------------------------------------

init(Req, _State) ->
    {cowboy_rest, rabbit_mgmt_headers:set_common_permission_headers(Req, ?MODULE), #context{}}.

variances(Req, Context) ->
    {[<<"accept-encoding">>, <<"origin">>], Req, Context}.

allowed_methods(ReqData, Context) ->
    {[<<"DELETE">>, <<"OPTIONS">>], ReqData, Context}.

resource_exists(ReqData, Context) ->
    case get_node(ReqData) of
        none       -> {true, ReqData, Context};
        {ok, Node} -> {lists:member(Node, rabbit_nodes:all_running()),
                       ReqData, Context}
    end.

delete_resource(ReqData, Context) ->
    case get_node(ReqData) of
        none  ->
            rabbit_mgmt_storage:reset_all();
        {ok, Node} ->
            rpc:call(Node, rabbit_mgmt_storage, reset, [])
    end,
    {true, ReqData, Context}.

is_authorized(ReqData, Context) ->
    rabbit_mgmt_util:is_authorized_admin(ReqData, Context).


get_node(ReqData) ->
    case rabbit_mgmt_util:id(node, ReqData) of
        none  ->
            none;
        Node0 ->
            Node = list_to_atom(binary_to_list(Node0)),
            {ok, Node}
    end.
