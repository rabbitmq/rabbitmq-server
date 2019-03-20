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
%% Copyright (c) 2007-2018 Pivotal Software, Inc.  All rights reserved.
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
