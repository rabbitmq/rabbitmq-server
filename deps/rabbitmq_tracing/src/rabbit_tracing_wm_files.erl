%%  The contents of this file are subject to the Mozilla Public License
%%  Version 1.1 (the "License"); you may not use this file except in
%%  compliance with the License. You may obtain a copy of the License
%%  at http://www.mozilla.org/MPL/
%%
%%  Software distributed under the License is distributed on an "AS IS"
%%  basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%%  the License for the specific language governing rights and
%%  limitations under the License.
%%
%%  The Original Code is RabbitMQ.
%%
%%  The Initial Developer of the Original Code is GoPivotal, Inc.
%%  Copyright (c) 2007-2013 GoPivotal, Inc.  All rights reserved.
%%

-module(rabbit_tracing_wm_files).

-export([init/1, to_json/2, content_types_provided/2, is_authorized/2]).

-include_lib("rabbitmq_management/include/rabbit_mgmt.hrl").
-include_lib("webmachine/include/webmachine.hrl").

%%--------------------------------------------------------------------

init(_Config) -> {ok, #context{}}.

content_types_provided(ReqData, Context) ->
   {[{"application/json", to_json}], ReqData, Context}.

to_json(ReqData, Context) ->
    rabbit_mgmt_util:reply(rabbit_tracing_files:list(), ReqData, Context).

is_authorized(ReqData, Context) ->
    rabbit_mgmt_util:is_authorized_admin(ReqData, Context).

%%--------------------------------------------------------------------
