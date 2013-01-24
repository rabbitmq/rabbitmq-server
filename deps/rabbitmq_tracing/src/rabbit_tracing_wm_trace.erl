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
%%   The Original Code is RabbitMQ.
%%
%%   The Initial Developer of the Original Code is VMware, Inc.
%%   Copyright (c) 2007-2013 VMware, Inc.  All rights reserved.

-module(rabbit_tracing_wm_trace).

-export([init/1, resource_exists/2, to_json/2,
         content_types_provided/2, content_types_accepted/2,
         is_authorized/2, allowed_methods/2, accept_content/2,
         delete_resource/2]).

-define(ERR, <<"Something went wrong trying to start the trace - check the "
               "logs.">>).

-include_lib("rabbitmq_management/include/rabbit_mgmt.hrl").
-include_lib("webmachine/include/webmachine.hrl").

%%--------------------------------------------------------------------
init(_Config) -> {ok, #context{}}.

content_types_provided(ReqData, Context) ->
   {[{"application/json", to_json}], ReqData, Context}.

content_types_accepted(ReqData, Context) ->
   {[{"application/json", accept_content}], ReqData, Context}.

allowed_methods(ReqData, Context) ->
    {['HEAD', 'GET', 'PUT', 'DELETE'], ReqData, Context}.

resource_exists(ReqData, Context) ->
    {case trace(ReqData) of
         not_found -> false;
         _         -> true
     end, ReqData, Context}.

to_json(ReqData, Context) ->
    rabbit_mgmt_util:reply(trace(ReqData), ReqData, Context).

accept_content(ReqData, Context) ->
    case rabbit_mgmt_util:vhost(ReqData) of
        not_found -> not_found;
        VHost     -> Name = rabbit_mgmt_util:id(name, ReqData),
                     rabbit_mgmt_util:with_decode(
                       [format], ReqData, Context,
                       fun([_], Trace) ->
                               case rabbit_tracing_traces:create(
                                      VHost, Name, Trace) of
                                   {ok, _} -> {true, ReqData, Context};
                                   _       -> rabbit_mgmt_util:bad_request(
                                                ?ERR, ReqData, Context)
                               end
                       end)
    end.

delete_resource(ReqData, Context) ->
    VHost = rabbit_mgmt_util:vhost(ReqData),
    Name = rabbit_mgmt_util:id(name, ReqData),
    ok = rabbit_tracing_traces:stop(VHost, Name),
    {true, ReqData, Context}.

is_authorized(ReqData, Context) ->
    rabbit_mgmt_util:is_authorized_admin(ReqData, Context).

%%--------------------------------------------------------------------

trace(ReqData) ->
    case rabbit_mgmt_util:vhost(ReqData) of
        not_found -> not_found;
        VHost     -> rabbit_tracing_traces:lookup(
                       VHost, rabbit_mgmt_util:id(name, ReqData))
    end.
