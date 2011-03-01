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
%% The Original Code is RabbitMQ Federation.
%%
%% The Initial Developer of the Original Code is VMware, Inc.
%% Copyright (c) 2007-2011 VMware, Inc.  All rights reserved.
%%

-module(rabbit_federation_db).

-export([init/0, sup_for_exchange/1, set_sup_for_exchange/2,
         forget_exchange/1]).

%% TODO get rid of this dets table, use mnesia
-define(DETS_NAME, rabbit_federation_exchange).

init() ->
    F = file(),
    {ok, F} = dets:open_file(F, [{type, set}]).

sup_for_exchange(X) ->
    [{_, Pid}] = dets:lookup(file(), X),
    Pid.

set_sup_for_exchange(X, Pid) ->
    ok = dets:insert(file(), {X, Pid}).

forget_exchange(X) ->
    true = dets:delete(file(), X).

%%----------------------------------------------------------------------------

file() ->
    rabbit_mnesia:dir() ++ "/" ++ ?DETS_NAME.
