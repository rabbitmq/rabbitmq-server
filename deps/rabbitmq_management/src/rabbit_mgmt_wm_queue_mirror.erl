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
-module(rabbit_mgmt_wm_queue_mirror).

-export([init/1, resource_exists/2, to_json/2,
         content_types_provided/2, content_types_accepted/2,
         is_authorized/2, allowed_methods/2, accept_content/2,
         delete_resource/2]).

-include("rabbit_mgmt.hrl").
-include_lib("webmachine/include/webmachine.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

%%--------------------------------------------------------------------
init(_Config) -> {ok, #context{}}.

content_types_provided(ReqData, Context) ->
   {[{"application/json", to_json}], ReqData, Context}.

content_types_accepted(ReqData, Context) ->
   {[{"application/json", accept_content}], ReqData, Context}.

allowed_methods(ReqData, Context) ->
    {['HEAD', 'GET', 'PUT', 'DELETE'], ReqData, Context}.

resource_exists(ReqData, Context) ->
    {exists(ReqData), ReqData, Context}.

to_json(ReqData, Context) ->
    rabbit_mgmt_util:reply([{node, n(ReqData)}], ReqData, Context).

accept_content(ReqData, Context) ->
    case exists(ReqData) of
        true  -> {true, ReqData, Context};
        false -> case rabbit_mgmt_wm_queue:queue(ReqData) of
                     not_found -> rabbit_mgmt_util:not_found(<<"no queue">>,
                                                             ReqData, Context);
                     _         -> sync(fun rabbit_mirror_queue_misc:add_slave/2,
                                       ReqData, Context)
                 end
    end.

delete_resource(ReqData, Context) ->
    sync(fun rabbit_mirror_queue_misc:drop_slave/2, ReqData, Context).

is_authorized(ReqData, Context) ->
    rabbit_mgmt_util:is_authorized_admin(ReqData, Context).

%%--------------------------------------------------------------------

exists(ReqData) ->
    case rabbit_mgmt_wm_queue:queue(ReqData) of
        not_found -> false;
        Q0        -> Q = rabbit_mgmt_format:strip_pids(
                           rabbit_mgmt_db:get_queue(Q0)),
                     Pids = proplists:get_value(mirror_nodes, Q, []),
                     lists:member(n(ReqData), Pids)
    end.

n(ReqData) ->
    list_to_atom(binary_to_list(rabbit_mgmt_util:id(node, ReqData))).

sync(Fun, ReqData, Context) ->
    QName = rabbit_misc:r(rabbit_mgmt_util:id(vhost, ReqData), queue,
                          rabbit_mgmt_util:id(queue, ReqData)),
    case Fun(QName, n(ReqData)) of
        ok ->
            %% If the master is hibernating we need to wake it up so
            %% we see an updated queue_stats with the mirror synced
            %% TODO we shouldn't have to do this
            rabbit_amqqueue:with(QName,
                                 fun(Q) -> rabbit_amqqueue:info(Q, []) end),
            {true, ReqData, Context};
        {error, E} ->
            rabbit_mgmt_util:bad_request(E, ReqData, Context)
    end.
