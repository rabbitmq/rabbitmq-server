%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License at
%% http://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
%% License for the specific language governing rights and limitations
%% under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2007-2019 Pivotal Software, Inc.  All rights reserved.
%%
-module(rabbit_prometheus_handler).

-export([init/2]).
-export([generate_response/2, content_types_provided/2, is_authorized/2]).

%% ===================================================================
%% Cowboy Handler Callbacks
%% ===================================================================

init(Req, _State) ->
  {cowboy_rest, Req, #{}}.

content_types_provided(ReqData, Context) ->
  {[
    {<<"*/*">>, generate_response}
   ], ReqData, Context}.

is_authorized(ReqData, Context) ->
    {true, ReqData, Context}.

%% ===================================================================
%% Private functions
%% ===================================================================

generate_response(ReqData, Context) ->
  {ok, Response, undefined} = prometheus_cowboy2_handler:init(ReqData, Context),
  {stop, Response, Context}.
