%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at http://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and
%% limitations under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is VMware, Inc.
%% Copyright (c) 2012-2013 VMware, Inc.  All rights reserved.
%%

-module(rabbit_ws_sup).
-behaviour(supervisor2).

-export([start_link/0, init/1, start_client/1]).

-define(SUP_NAME, ?MODULE).

%%----------------------------------------------------------------------------

-spec start_link() -> ignore | {'ok', pid()} | {'error', any()}.
start_link() ->
    supervisor2:start_link({local, ?SUP_NAME}, ?MODULE, []).

init([]) ->
    {ok, {{simple_one_for_one_terminate, 0, 1},
          [{client, {rabbit_ws_client_sup, start_client, []},
            temporary, infinity, supervisor, [rabbit_ws_client_sup]}]}}.

start_client(Params) ->
    supervisor2:start_child(?SUP_NAME, [Params]).
