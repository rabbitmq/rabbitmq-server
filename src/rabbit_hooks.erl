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
%%   The Initial Developers of the Original Code are LShift Ltd,
%%   Cohesive Financial Technologies LLC, and Rabbit Technologies Ltd.
%%
%%   Portions created before 22-Nov-2008 00:00:00 GMT by LShift Ltd,
%%   Cohesive Financial Technologies LLC, or Rabbit Technologies Ltd
%%   are Copyright (C) 2007-2008 LShift Ltd, Cohesive Financial
%%   Technologies LLC, and Rabbit Technologies Ltd.
%%
%%   Portions created by LShift Ltd are Copyright (C) 2007-2009 LShift
%%   Ltd. Portions created by Cohesive Financial Technologies LLC are
%%   Copyright (C) 2007-2009 Cohesive Financial Technologies
%%   LLC. Portions created by Rabbit Technologies Ltd are Copyright
%%   (C) 2007-2009 Rabbit Technologies Ltd.
%%
%%   All Rights Reserved.
%%
%%   Contributor(s): ______________________________________.
%%

-module(rabbit_hooks).

-behaviour(gen_server).

-export([start_link/0]).
-export([subscribe/2, unsubscribe/2, trigger/2]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).

-define(TableName, rabbit_hooks).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

subscribe(Hook, Handler) ->
    gen_server:call(?MODULE, {add_hook, Hook, Handler}).

unsubscribe(Hook, Handler) ->
    gen_server:call(?MODULE, {remove_hook, Hook, Handler}).

trigger(Hook, Args) ->
    Hooks = get_current_hooks(Hook),
    [catch H(Args) || H <- Hooks],
    ok.


%% Gen Server Implementation
init([]) ->
    ets:new(?TableName, [named_table]),
    {ok, []}.

handle_call({add_hook, Hook, Handler}, _, State) ->
    Current = get_current_hooks(Hook),
    Updated = Current ++ [Handler],
    ets:insert(?TableName, {Hook, Updated}),
    {reply, ok, State};

handle_call({remove_hook, Hook, Handler}, _, State) ->
    Current = get_current_hooks(Hook),
    Updated = [H || H <- Current, not(H == Handler)],
    ets:insert(?TableName, {Hook, Updated}),
    {reply, ok, State};
 
handle_call(_, _, State) ->
    {reply, ok, State}.

handle_cast(_, State) ->
    {noreply, State}.

handle_info(_, State) ->
    {noreply, State}.

terminate(_, _) ->
    ok.

code_change(_, State, _) ->
    {ok, State}.

%% Helper Methods
get_current_hooks(Hook) ->
  case ets:lookup(?TableName, Hook) of
      []          -> [];
      [{Hook, C}] -> C
  end.
