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
%%   Portions created by LShift Ltd are Copyright (C) 2007-2010 LShift
%%   Ltd. Portions created by Cohesive Financial Technologies LLC are
%%   Copyright (C) 2007-2010 Cohesive Financial Technologies
%%   LLC. Portions created by Rabbit Technologies Ltd are Copyright
%%   (C) 2007-2010 Rabbit Technologies Ltd.
%%
%%   All Rights Reserved.
%%
%%   Contributor(s): ______________________________________.
%%

-module(rabbit_hooks).

-export([start/0]).
-export([subscribe/3, unsubscribe/2, trigger/2, notify_remote/5]).

-define(TableName, rabbit_hooks).

-ifdef(use_specs).

-spec(start/0 :: () -> 'ok').
-spec(subscribe/3 :: (atom(), atom(), {atom(), atom(), list()}) -> 'ok').
-spec(unsubscribe/2 :: (atom(), atom()) -> 'ok').
-spec(trigger/2 :: (atom(), list()) -> 'ok').
-spec(notify_remote/5 :: (atom(), atom(), list(), pid(), list()) -> 'ok').

-endif.

start() ->
    ets:new(?TableName, [bag, public, named_table]),
    ok.

subscribe(Hook, HandlerName, Handler) ->
    ets:insert(?TableName, {Hook, HandlerName, Handler}),
    ok.

unsubscribe(Hook, HandlerName) ->
    ets:match_delete(?TableName, {Hook, HandlerName, '_'}),
    ok.

trigger(Hook, Args) ->
    Hooks = ets:lookup(?TableName, Hook),
    [case catch apply(M, F, [Hook, Name, Args | A]) of
        {'EXIT', Reason} ->
            rabbit_log:warning("Failed to execute handler ~p for hook ~p: ~p",
                               [Name, Hook, Reason]);
        _ -> ok
     end || {_, Name, {M, F, A}} <- Hooks],
    ok.

notify_remote(Hook, HandlerName, Args, Pid, PidArgs) ->
    Pid ! {rabbitmq_hook, [Hook, HandlerName, Args | PidArgs]},
    ok.
