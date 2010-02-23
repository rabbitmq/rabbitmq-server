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

-module(rabbit_exchange_type_registry).

-behaviour(gen_server).

-export([start_link/0]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-export([register/2, binary_to_type/1, lookup_module/1]).

-define(SERVER, ?MODULE).
-define(ETS_NAME, ?MODULE).

-ifdef(use_specs).

-spec(start_link/0 :: () -> 'ignore' | {'error', term()} | {'ok', pid()}).
-spec(register/2 :: (binary(), atom()) -> 'ok').
-spec(binary_to_type/1 :: (binary()) -> atom() | {'error', 'not_found'}).
-spec(lookup_module/1 :: (atom()) -> {'ok', atom()} | {'error', 'not_found'}).

-endif.

%%---------------------------------------------------------------------------

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

%%---------------------------------------------------------------------------

register(TypeName, ModuleName) ->
    gen_server:call(?SERVER, {register, TypeName, ModuleName}).

%% This is used with user-supplied arguments (e.g., on exchange
%% declare), so we restrict it to existing atoms only.  This means it
%% can throw a badarg, indicating that the type cannot have been
%% registered.
binary_to_type(TypeBin) when is_binary(TypeBin) ->
    case catch list_to_existing_atom(binary_to_list(TypeBin)) of
        {'EXIT', {badarg, _}} -> {error, not_found};
        TypeAtom              -> TypeAtom
    end.

lookup_module(T) when is_atom(T) ->
    case ets:lookup(?ETS_NAME, T) of
        [{_, Module}] ->
            {ok, Module};
        [] ->
            {error, not_found}
    end.

%%---------------------------------------------------------------------------

internal_binary_to_type(TypeBin) when is_binary(TypeBin) ->
    list_to_atom(binary_to_list(TypeBin)).

internal_register(TypeName, ModuleName)
  when is_binary(TypeName), is_atom(ModuleName) ->
    ok = sanity_check_module(ModuleName),
    true = ets:insert(?ETS_NAME,
                      {internal_binary_to_type(TypeName), ModuleName}),
    ok.

sanity_check_module(Module) ->
    case catch lists:member(rabbit_exchange_type,
                            lists:flatten(
                              [Bs || {Attr, Bs} <-
                                         Module:module_info(attributes),
                                     Attr =:= behavior orelse
                                         Attr =:= behaviour])) of
        {'EXIT', {undef, _}}  -> {error, not_module};
        false                 -> {error, not_exchange_type};
        true                  -> ok
    end.

%%---------------------------------------------------------------------------

init([]) ->
    ?ETS_NAME = ets:new(?ETS_NAME, [protected, set, named_table]),
    {ok, none}.

handle_call({register, TypeName, ModuleName}, _From, State) ->
    ok = internal_register(TypeName, ModuleName),
    {reply, ok, State};
handle_call(Request, _From, State) ->
    {stop, {unhandled_call, Request}, State}.

handle_cast(Request, State) ->
    {stop, {unhandled_cast, Request}, State}.

handle_info(Message, State) ->
    {stop, {unhandled_info, Message}, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
