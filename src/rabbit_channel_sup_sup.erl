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
%% Copyright (c) 2007-2012 VMware, Inc.  All rights reserved.
%%

-module(rabbit_channel_sup_sup).

-behaviour(supervisor2).

-export([start_link/0, start_channel/2]).

-export([init/1]).

%%----------------------------------------------------------------------------

-ifdef(use_specs).

-spec(start_link/0 :: () -> rabbit_types:ok_pid_or_error()).
-spec(start_channel/2 :: (pid(), rabbit_channel_sup:start_link_args()) ->
                              {'ok', pid(), {pid(), any()}}).

-endif.

%%----------------------------------------------------------------------------

start_link() ->
    supervisor2:start_link(?MODULE, []).

start_channel(Pid, Args) ->
    supervisor2:start_child(Pid, [Args]).

%%----------------------------------------------------------------------------

init([]) ->
    {ok, {{simple_one_for_one_terminate, 0, 1},
          [{channel_sup, {rabbit_channel_sup, start_link, []},
            temporary, infinity, supervisor, [rabbit_channel_sup]}]}}.
