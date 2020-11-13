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
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2007-2017 Pivotal Software, Inc.  All rights reserved.
%%

-module(rabbit_amqp1_0_session_sup_sup).

-behaviour(supervisor2).

-export([start_link/0, start_session/2]).

-export([init/1]).

%% It would be much nicer if rabbit_channel_sup_sup was parameterised
%% on the module.

%%----------------------------------------------------------------------------

-spec start_link() -> rabbit_types:ok_pid_or_error().
-spec start_session(pid(), rabbit_amqp1_0_session_sup:start_link_args()) ->
                              {'ok', pid(), pid()}.

%%----------------------------------------------------------------------------

start_link() ->
    supervisor2:start_link(?MODULE, []).

start_session(Pid, Args) ->
    supervisor2:start_child(Pid, [Args]).

%%----------------------------------------------------------------------------

init([]) ->
    {ok, {{simple_one_for_one, 0, 1},
          [{session_sup, {rabbit_amqp1_0_session_sup, start_link, []},
            temporary, infinity, supervisor, [rabbit_amqp1_0_session_sup]}]}}.
