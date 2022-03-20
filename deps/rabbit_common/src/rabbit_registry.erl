%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.
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

-spec start_link() -> rabbit_types:ok_pid_or_error().
-spec register(atom(), binary(), atom()) -> 'ok'.
-spec unregister(atom(), binary()) -> 'ok'.
-spec binary_to_type(binary()) -> atom() | rabbit_types:error('not_found').
-spec lookup_module(atom(), atom()) ->
          rabbit_types:ok_or_error2(atom(), 'not_found').
-spec lookup_all(atom()) -> [{atom(), atom()}].

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
    ClassModule = class_module(Class),
    Type        = internal_binary_to_type(TypeName),
    RegArg      = {{Class, Type}, ModuleName},
    ok = sanity_check_module(ClassModule, ModuleName),
    true = ets:insert(?ETS_NAME, RegArg),
    conditional_register(RegArg),
    ok = ClassModule:added_to_rabbit_registry(Type, ModuleName),
    ok.

internal_unregister(Class, TypeName) ->
    ClassModule = class_module(Class),
    Type        = internal_binary_to_type(TypeName),
    UnregArg    = {Class, Type},
    conditional_unregister(UnregArg),
    true = ets:delete(?ETS_NAME, UnregArg),
    ok = ClassModule:removed_from_rabbit_registry(Type),
    ok.

%% register exchange decorator route callback only when implemented,
%% in order to avoid unnecessary decorator calls on the fast
%% publishing path
conditional_register({{exchange_decorator, Type}, ModuleName}) ->
    case erlang:function_exported(ModuleName, route, 2) of
        true  -> true = ets:insert(?ETS_NAME,
                                   {{exchange_decorator_route, Type},
                                    ModuleName});
        false -> ok
    end;
conditional_register(_) ->
    ok.

conditional_unregister({exchange_decorator, Type}) ->
    true = ets:delete(?ETS_NAME, {exchange_decorator_route, Type}),
    ok;
conditional_unregister(_) ->
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


% Registry class modules. There should exist module for each registry class.
% Class module should be behaviour (export behaviour_info/1) and implement
% rabbit_registry_class behaviour itself: export added_to_rabbit_registry/2
% and removed_from_rabbit_registry/1 functions.
class_module(exchange)            -> rabbit_exchange_type;
class_module(auth_mechanism)      -> rabbit_auth_mechanism;
class_module(runtime_parameter)   -> rabbit_runtime_parameter;
class_module(exchange_decorator)  -> rabbit_exchange_decorator;
class_module(queue_decorator)     -> rabbit_queue_decorator;
class_module(policy_validator)    -> rabbit_policy_validator;
class_module(operator_policy_validator) -> rabbit_policy_validator;
class_module(policy_merge_strategy)     -> rabbit_policy_merge_strategy;
class_module(ha_mode)                   -> rabbit_mirror_queue_mode;
class_module(channel_interceptor)       -> rabbit_channel_interceptor;
class_module(queue_master_locator)      -> rabbit_queue_master_locator.

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
