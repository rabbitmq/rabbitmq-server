%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%
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
    {ok, Content} = rabbit_misc:raw_read_file(Path),
    Content.

delete_resource(ReqData, Context) ->
    Name = rabbit_mgmt_util:id(name, ReqData),
    ok = rabbit_tracing_util:apply_on_node(ReqData, Context, rabbit_tracing_files,
                                           delete, [Name]),
    {true, ReqData, Context}.

is_authorized(ReqData, Context) ->
    rabbit_mgmt_util:is_authorized_admin(ReqData, Context).

