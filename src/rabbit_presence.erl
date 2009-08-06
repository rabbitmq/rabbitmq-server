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

-module(rabbit_presence).

-behaviour(gen_server).

-include("rabbit.hrl").
-include("rabbit_framing.hrl").

-export([start_link/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).


%% Gen Server Implementation
init([]) ->
    attach(sync, vhost_create),
    attach(async, [queue_startup, queue_shutdown]),
    {ok, []}.

handle_call({vhost_create, [VHostPath]}, _, State) ->
    rabbit_exchange:declare(rabbit_misc:r(VHostPath, exchange, 
                                          <<"amq.rabbitmq.presence">>),
                            topic, true, false, []),
    {reply, ok, State};

handle_call(_, _, State) ->
    {reply, ok, State}.

handle_cast({queue_startup, [QName = #resource{}]}, State) ->
    emit_presence(QName, <<"startup">>),
    {noreply, State};
handle_cast({queue_shutdown, [QName = #resource{}]}, State) ->
    emit_presence(QName, <<"shutdown">>),
    {noreply, State};
handle_cast(_Msg, State) ->
    io:format("Unknown cast ~p~n", [_Msg]),
    {noreply, State}.

handle_info(_, State) ->
    {noreply, State}.

terminate(_, _) ->
    ok.

code_change(_, State, _) ->
    {ok, State}.

%% Helper Methods
attach(InvokeMethod, Hooks) when is_list(Hooks) ->
    [attach(InvokeMethod, Hook) || Hook <- Hooks];
attach(InvokeMethod, HookName) when is_atom(HookName) ->
    rabbit_hooks:subscribe(HookName, handler(InvokeMethod, HookName)).

handler(async, HookName) ->
    fun(Args) -> gen_server:cast(?MODULE, {HookName, Args}) end;
handler(sync, HookName) ->
    fun(Args) -> gen_server:call(?MODULE, {HookName, Args}) end.

escape_for_routing_key(K) when is_binary(K) ->
    list_to_binary(escape_for_routing_key1(binary_to_list(K))).

escape_for_routing_key1([]) ->
    [];
escape_for_routing_key1([Ch | Rest]) ->
    Tail = escape_for_routing_key1(Rest),
    case Ch of
        $# -> "%23" ++ Tail;
        $% -> "%25" ++ Tail;
        $* -> "%2a" ++ Tail;
        $. -> "%2e" ++ Tail;
        _ -> [Ch | Tail]
    end.

emit_presence(Resource = #resource{kind = KindAtom, name = InstanceBin},
              EventBin) ->
    ClassBin = list_to_binary(atom_to_list(KindAtom)),
    XName = rabbit_misc:r(Resource, exchange, <<"amq.rabbitmq.presence">>),
    EscapedInstance = escape_for_routing_key(InstanceBin),
    RK = list_to_binary(["presence.", ClassBin, ".", EscapedInstance,
                         ".", EventBin]),
    Body = list_to_binary([ClassBin, ".", EventBin, ".", EscapedInstance]),
    Message = rabbit_basic:message(XName, RK, #'P_basic'{}, Body),
    Delivery = rabbit_basic:delivery(false, false, none, Message),
    _Ignored = case rabbit_exchange:lookup(XName) of
           {ok, Exchange} ->
               rabbit_exchange:publish(Exchange, Delivery);
           {error, Error} -> {error, Error}
    end,
    ok.
