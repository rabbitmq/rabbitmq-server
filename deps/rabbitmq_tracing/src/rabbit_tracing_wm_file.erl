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
%%   The Initial Developer of the Original Code is GoPivotal, Inc.
%%   Copyright (c) 2007-2013 GoPivotal, Inc.  All rights reserved.

-module(rabbit_tracing_wm_file).

-export([init/1, resource_exists/2, serve/2, content_types_provided/2,
         is_authorized/2, allowed_methods/2, delete_resource/2]).

-include_lib("rabbitmq_management/include/rabbit_mgmt.hrl").
-include_lib("webmachine/include/webmachine.hrl").

%%--------------------------------------------------------------------
init(_Config) -> {ok, #context{}}.

content_types_provided(ReqData, Context) ->
   {[{"text/plain", serve}], ReqData, Context}.

allowed_methods(ReqData, Context) ->
    {['HEAD', 'GET', 'DELETE'], ReqData, Context}.

resource_exists(ReqData, Context) ->
    Name = rabbit_mgmt_util:id(name, ReqData),
    {rabbit_tracing_files:exists(Name), ReqData, Context}.

serve(ReqData, Context) ->
    Name = rabbit_mgmt_util:id(name, ReqData),
    {ok, Content} = file:read_file(rabbit_tracing_files:full_path(Name)),
    {Content, ReqData, Context}.

delete_resource(ReqData, Context) ->
    Name = rabbit_mgmt_util:id(name, ReqData),
    ok = rabbit_tracing_files:delete(Name),
    {true, ReqData, Context}.

is_authorized(ReqData, Context) ->
    rabbit_mgmt_util:is_authorized_admin(ReqData, Context).

