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
%%   Copyright (c) 2007-2017 Pivotal Software, Inc.  All rights reserved.

-module(rabbit_tracing_wm_file).

-export([init/2, resource_exists/2, serve/2, content_types_provided/2,
         is_authorized/2, allowed_methods/2, delete_resource/2]).
-export([serve/1]).

-include_lib("rabbitmq_management_agent/include/rabbit_mgmt_records.hrl").

%%--------------------------------------------------------------------
init(Req, _State) ->
    {cowboy_rest, rabbit_mgmt_cors:set_headers(Req, ?MODULE), #context{}}.


content_types_provided(ReqData, Context) ->
   {[{<<"text/plain">>, serve}], ReqData, Context}.

allowed_methods(ReqData, Context) ->
    {[<<"HEAD">>, <<"GET">>, <<"DELETE">>], ReqData, Context}.

resource_exists(ReqData, Context) ->
    Name = rabbit_mgmt_util:id(name, ReqData),
    Exists = rabbit_tracing_util:apply_on_node(ReqData, Context, rabbit_tracing_files,
                                               exists, [Name]),
    {Exists, ReqData, Context}.

serve(ReqData, Context) ->
    Name = rabbit_mgmt_util:id(name, ReqData),
    Content = rabbit_tracing_util:apply_on_node(ReqData, Context,
                                                rabbit_tracing_wm_file,
                                                serve, [Name]),
    {Content, ReqData, Context}.

serve(Name) ->
    Path = rabbit_tracing_files:full_path(Name),
    {ok, Content} = file:read_file(Path),
    Content.

delete_resource(ReqData, Context) ->
    Name = rabbit_mgmt_util:id(name, ReqData),
    ok = rabbit_tracing_util:apply_on_node(ReqData, Context, rabbit_tracing_files,
                                           delete, [Name]),
    {true, ReqData, Context}.

is_authorized(ReqData, Context) ->
    rabbit_mgmt_util:is_authorized_admin(ReqData, Context).

