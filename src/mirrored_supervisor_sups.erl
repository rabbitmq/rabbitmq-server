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
%% Copyright (c) 2011-2019 Pivotal Software, Inc.  All rights reserved.
%%

-module(mirrored_supervisor_sups).

-define(SUPERVISOR, supervisor2).
-define(GS_MODULE,  mirrored_supervisor).

-behaviour(?SUPERVISOR).

-export([init/1]).

%%----------------------------------------------------------------------------

init({overall, _Group, _TxFun, ignore}) -> ignore;
init({overall,  Group,  TxFun, {ok, {Restart, ChildSpecs}}}) ->
    %% Important: Delegate MUST start before Mirroring so that when we
    %% shut down from above it shuts down last, so Mirroring does not
    %% see it die.
    %%
    %% See comment in handle_info('DOWN', ...) in mirrored_supervisor
    {ok, {{one_for_all, 0, 1},
          [{delegate, {?SUPERVISOR, start_link, [?MODULE, {delegate, Restart}]},
            temporary, 16#ffffffff, supervisor, [?SUPERVISOR]},
           {mirroring, {?GS_MODULE, start_internal, [Group, TxFun, ChildSpecs]},
            permanent, 16#ffffffff, worker, [?MODULE]}]}};


init({delegate, Restart}) ->
    {ok, {Restart, []}}.
