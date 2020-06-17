%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at https://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and
%% limitations under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2007-2020 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_restartable_sup).

-behaviour(supervisor2).

-export([start_link/3]).

-export([init/1]).

-include("rabbit.hrl").

-define(DELAY, 2).

%%----------------------------------------------------------------------------

-spec start_link(atom(), rabbit_types:mfargs(), boolean()) ->
                           rabbit_types:ok_pid_or_error().

start_link(Name, {_M, _F, _A} = Fun, Delay) ->
    supervisor2:start_link({local, Name}, ?MODULE, [Fun, Delay]).

init([{Mod, _F, _A} = Fun, Delay]) ->
    {ok, {{one_for_one, 10, 10},
          [{Mod, Fun, case Delay of
                          true  -> {transient, 1};
                          false -> transient
                      end, ?WORKER_WAIT, worker, [Mod]}]}}.
