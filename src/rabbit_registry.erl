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

-module(rabbit_registry).

-behaviour(gen_server).

-export([start_link/0]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).

-export([register/3, unregister/2,
         binary_to_type/1, lookup_module/2, lookup_all/1]).

-define(SERVER, ?MODULE).
-define(ETS_NAME, ?MODULE).

-ifdef(use_specs).

-spec(start_link/0 :: () -> rabbit_types:ok_pid_or_error()).
-spec(register/3 :: (atom(), binary(), atom()) -> 'ok').
-spec(unregister/2 :: (atom(), binary()) -> 'ok').
-spec(binary_to_type/1 ::
        (binary()) -> atom() | rabbit_types:error('not_found')).
-spec(lookup_module/2 ::
        (atom(), atom()) -> rabbit_types:ok_or_error2(atom(), 'not_found')).
-spec(lookup_all/1 :: (atom()) -> [{atom(), atom()}]).

-endif.

%%---------------------------------------------------------------------------

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

%%---------------------------------------------------------------------------

register(Class, TypeName, ModuleName) ->
    gen_server:call(?SERVER, {register, Class, TypeName, ModuleName}, infinity).

unregister(Class, TypeName) ->
    gen_server:call(?SERVER, {unregister, Class, TypeName}, infinity).

%% This is used with user-supplied arguments (e.g., on exchange
%% declare), so we restrict it to existing atoms only.  This means it
%% can throw a badarg, indicating that the type cannot have been
%% registered.
binary_to_type(TypeBin) when is_binary(TypeBin) ->
    case catch list_to_existing_atom(binary_to_list(TypeBin)) of
        {'EXIT', {badarg, _}} -> {error, not_found};
        TypeAtom              -> TypeAtom
    end.

lookup_module(Class, T) when is_atom(T) ->
    case ets:lookup(?ETS_NAME, {Class, T}) of
        [{_, Module}] ->
            {ok, Module};
        [] ->
            {error, not_found}
    end.

lookup_all(Class) ->
    [{K, V} || [K, V] <- ets:match(?ETS_NAME, {{Class, '$1'}, '$2'})].

%%---------------------------------------------------------------------------

internal_binary_to_type(TypeBin) when is_binary(TypeBin) ->
    list_to_atom(binary_to_list(TypeBin)).

internal_register(Class, TypeName, ModuleName)
  when is_atom(Class), is_binary(TypeName), is_atom(ModuleName) ->
    ok = sanity_check_module(class_module(Class), ModuleName),
    true = ets:insert(?ETS_NAME,
                      {{Class, internal_binary_to_type(TypeName)}, ModuleName}),
    ok.

internal_unregister(Class, TypeName) ->
    true = ets:delete(?ETS_NAME, {Class, internal_binary_to_type(TypeName)}),
    ok.

sanity_check_module(ClassModule, Module) ->
    case catch lists:member(ClassModule,
                            lists:flatten(
                              [Bs || {Attr, Bs} <-
                                         Module:module_info(attributes),
                                     Attr =:= behavior orelse
                                         Attr =:= behaviour])) of
        {'EXIT', {undef, _}}  -> {error, not_module};
        false                 -> {error, {not_type, ClassModule}};
        true                  -> ok
    end.

class_module(exchange)          -> rabbit_exchange_type;
class_module(auth_mechanism)    -> rabbit_auth_mechanism;
class_module(runtime_parameter) -> rabbit_runtime_parameter.

%%---------------------------------------------------------------------------

init([]) ->
    ?ETS_NAME = ets:new(?ETS_NAME, [protected, set, named_table]),
    {ok, none}.

handle_call({register, Class, TypeName, ModuleName}, _From, State) ->
    ok = internal_register(Class, TypeName, ModuleName),
    {reply, ok, State};

handle_call({unregister, Class, TypeName}, _From, State) ->
    ok = internal_unregister(Class, TypeName),
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
