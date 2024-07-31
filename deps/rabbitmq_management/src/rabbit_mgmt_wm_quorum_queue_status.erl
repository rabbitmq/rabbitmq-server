%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2024 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

%% An HTTP API counterpart of 'rabbitmq-diagnostics check_if_node_is_quorum_critical'
-module(rabbit_mgmt_wm_quorum_queue_status).

-export([init/2, to_json/2, content_types_provided/2, is_authorized/2, allowed_methods/2]).
-export([resource_exists/2]).
-export([variances/2]).

-include_lib("rabbitmq_management_agent/include/rabbit_mgmt_records.hrl").

%%--------------------------------------------------------------------

init(Req, _State) ->
    {cowboy_rest, rabbit_mgmt_headers:set_common_permission_headers(Req, ?MODULE), #context{}}.

variances(Req, Context) ->
    {[<<"accept-encoding">>, <<"origin">>], Req, Context}.

allowed_methods(ReqData, Context) ->
  {[<<"GET">>, <<"OPTIONS">>], ReqData, Context}.

content_types_provided(ReqData, Context) ->
   {rabbit_mgmt_util:responder_map(to_json), ReqData, Context}.

resource_exists(ReqData, Context) ->
    {case queue(ReqData) of
         not_found -> false;
         _         -> true
     end, ReqData, Context}.

to_json(ReqData, Context) ->
    case  queue(ReqData) of
        {error, Reason} ->
            failure(Reason, ReqData, Context);
        Res ->
            rabbit_mgmt_util:reply(Res, ReqData, Context)
    end.

queue(ReqData) ->
    case rabbit_mgmt_util:vhost(ReqData) of
        not_found -> not_found;
        VHost     -> queue(VHost, rabbit_mgmt_util:id(queue, ReqData))
    end.

queue(VHost, QName) ->
    Name = rabbit_misc:r(VHost, queue, QName),
    case rabbit_amqqueue:lookup(Name) of
        {ok, _}            -> rabbit_quorum_queue:status(VHost, QName);
        {error, not_found} -> not_found
    end.


failure(Reason, ReqData, Context) ->
    {Response, ReqData1, Context1} = rabbit_mgmt_util:reply([{status, failed},
                                                             {reason, Reason}],
                                                            ReqData, Context),
    {stop, cowboy_req:reply(503, #{}, Response, ReqData1), Context1}.

is_authorized(ReqData, Context) ->
    rabbit_mgmt_util:is_authorized(ReqData, Context).
