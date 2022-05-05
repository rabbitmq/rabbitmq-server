%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_runtime_parameters).

%% Runtime parameters are bits of configuration that are
%% set, as the name implies, at runtime and not in the config file.
%%
%% The benefits of storing some bits of configuration at runtime vary:
%%
%%  * Some parameters are vhost-specific
%%  * Others are specific to individual nodes
%%  * ...or even queues, exchanges, etc
%%
%% The most obvious use case for runtime parameters is policies but
%% there are others:
%%
%% * Plugin-specific parameters that only make sense at runtime,
%%   e.g. Federation and Shovel link settings
%% * Exchange and queue decorators
%%
%% Parameters are grouped by components, e.g. <<"policy">> or <<"shovel">>.
%% Components are mapped to modules that perform validation.
%% Runtime parameter values are then looked up by the modules that
%% need to use them.
%%
%% Parameters are stored in Mnesia and can be global. Their changes
%% are broadcasted over rabbit_event.
%%
%% Global parameters keys are atoms and values are JSON documents.
%%
%% See also:
%%
%%  * rabbit_policies
%%  * rabbit_policy
%%  * rabbit_registry
%%  * rabbit_event

-include_lib("khepri/include/khepri.hrl").

-include_lib("rabbit_common/include/rabbit.hrl").

-export([parse_set/5, set/5, set_any/5, clear/4, clear_any/4, list/0, list/1,
         list_component/1, list/2, list_formatted/1, list_formatted/3,
         lookup/3, value/3, value/4, info_keys/0, clear_component/2]).

-export([parse_set_global/3, set_global/3, value_global/1, value_global/2,
         list_global/0, list_global_formatted/0, list_global_formatted/2,
         lookup_global/1, global_info_keys/0, clear_global/2]).

-export([clear_data_in_khepri/0,
         mnesia_write_to_khepri/1,
         mnesia_delete_to_khepri/1]).

%%----------------------------------------------------------------------------

-type ok_or_error_string() :: 'ok' | {'error_string', string()}.
-type ok_thunk_or_error_string() :: ok_or_error_string() | fun(() -> 'ok').

%%---------------------------------------------------------------------------

-import(rabbit_misc, [pget/2]).

-define(TABLE, rabbit_runtime_parameters).

%%---------------------------------------------------------------------------

-spec parse_set(rabbit_types:vhost(), binary(), binary(), string(),
                rabbit_types:user() | rabbit_types:username() | 'none')
               -> ok_or_error_string().

parse_set(_, <<"policy">>, _, _, _) ->
    {error_string, "policies may not be set using this method"};
parse_set(VHost, Component, Name, String, User) ->
    Definition = rabbit_data_coercion:to_binary(String),
    case rabbit_json:try_decode(Definition) of
        {ok, Term} when is_map(Term) -> set(VHost, Component, Name, maps:to_list(Term), User);
        {ok, Term} -> set(VHost, Component, Name, Term, User);
        {error, Reason} ->
            {error_string,
                rabbit_misc:format("JSON decoding error. Reason: ~ts", [Reason])}
    end.

-spec set(rabbit_types:vhost(), binary(), binary(), term(),
                rabbit_types:user() | rabbit_types:username() | 'none')
         -> ok_or_error_string().

set(_, <<"policy">>, _, _, _) ->
    {error_string, "policies may not be set using this method"};
set(VHost, Component, Name, Term, User) ->
    set_any(VHost, Component, Name, Term, User).

parse_set_global(Name, String, ActingUser) ->
    Definition = rabbit_data_coercion:to_binary(String),
    case rabbit_json:try_decode(Definition) of
        {ok, Term} when is_map(Term) -> set_global(Name, maps:to_list(Term), ActingUser);
        {ok, Term} -> set_global(Name, Term, ActingUser);
        {error, Reason} ->
            {error_string,
                rabbit_misc:format("JSON decoding error. Reason: ~ts", [Reason])}
    end.

-spec set_global(atom(), term(), rabbit_types:username()) -> 'ok'.

set_global(Name, Term, ActingUser)  ->
    NameAsAtom = rabbit_data_coercion:to_atom(Name),
    rabbit_log:debug("Setting global parameter '~s' to ~p", [NameAsAtom, Term]),
    record_update(NameAsAtom, Term),
    event_notify(parameter_set, none, global, [{name,  NameAsAtom},
                                               {value, Term},
                                               {user_who_performed_action, ActingUser}]),
    ok.

format_error(L) ->
    {error_string, rabbit_misc:format_many([{"Validation failed~n", []} | L])}.

-spec set_any(rabbit_types:vhost(), binary(), binary(), term(),
              rabbit_types:user() | rabbit_types:username() | 'none')
             -> ok_or_error_string().

set_any(VHost, Component, Name, Term, User) ->
    case set_any0(VHost, Component, Name, Term, User) of
        ok          -> ok;
        {errors, L} -> format_error(L)
    end.

set_any0(VHost, Component, Name, Term, User) ->
    rabbit_log:debug("Asked to set or update runtime parameter '~s' in vhost '~s' "
                     "for component '~s', value: ~p",
                     [Name, VHost, Component, Term]),
    case lookup_component(Component) of
        {ok, Mod} ->
            case flatten_errors(
                   Mod:validate(VHost, Component, Name, Term, get_user(User))) of
                ok ->
                    case record_update(VHost, Component, Name, Term) of
                        {old, Term} ->
                            ok;
                        _           ->
                            ActingUser = get_username(User),
                            event_notify(
                              parameter_set, VHost, Component,
                              [{name,  Name},
                               {value, Term},
                               {user_who_performed_action, ActingUser}]),
                            Mod:notify(VHost, Component, Name, Term, ActingUser)
                    end,
                    ok;
                E ->
                    E
            end;
        E ->
            E
    end.

%% Validate only an user record as expected by the API before #rabbitmq-event-exchange-10
get_user(#user{} = User) ->
    User;
get_user(_) ->
    none.

get_username(#user{username = Username}) ->
    Username;
get_username(none) ->
    ?INTERNAL_USER;
get_username(Any) ->
    Any.

record_update(Key, Term) ->
    rabbit_khepri:try_mnesia_or_khepri(
      fun() -> mnesia_update(Key, Term) end,
      fun() -> khepri_update(Key, Term) end).

record_update(VHost, Comp, Name, Term) ->
    rabbit_khepri:try_mnesia_or_khepri(
      fun() -> mnesia_update(VHost, Comp, Name, Term) end,
      fun() -> khepri_update(VHost, Comp, Name, Term) end).

mnesia_update(Key, Term) ->
    rabbit_misc:execute_mnesia_transaction(mnesia_update_fun(Key, Term)).

mnesia_update(VHost, Comp, Name, Term) ->
    rabbit_misc:execute_mnesia_transaction(
      rabbit_vhost:with_in_mnesia(
        VHost, mnesia_update_fun({VHost, Comp, Name}, Term))).

mnesia_update_fun(Key, Term) ->
    fun () ->
            Res = case mnesia:read(?TABLE, Key, read) of
                      []       -> new;
                      [Params] -> {old, Params#runtime_parameters.value}
                      end,
            ok = mnesia:write(?TABLE, c(Key, Term), write),
            Res
    end.

khepri_update(Key, Term) ->
    rabbit_khepri:transaction(khepri_update_fun(Key, Term)).

khepri_update(VHost, Comp, Name, Term) ->
    rabbit_khepri:transaction(
      rabbit_vhost:with_in_khepri(
        VHost, khepri_update_fun({VHost, Comp, Name}, Term))).

khepri_update_fun(Key, Term) ->
    Path = khepri_rp_path(Key),
    fun () ->
            case khepri_tx:put(Path, c(Key, Term)) of
                {ok, #{Path := #{data := Params}}} ->
                    {old, Params#runtime_parameters.value};
                {ok, _} ->
                    new
            end
    end.

-spec clear(rabbit_types:vhost(), binary(), binary(), rabbit_types:username())
           -> ok_thunk_or_error_string().

clear(_, <<"policy">> , _, _) ->
    {error_string, "policies may not be cleared using this method"};
clear(VHost, Component, Name, ActingUser) ->
    clear_any(VHost, Component, Name, ActingUser).

clear_global(Key, ActingUser) ->
    KeyAsAtom = rabbit_data_coercion:to_atom(Key),
    Notify = fun() ->
                    event_notify(parameter_set, none, global,
                                 [{name,  KeyAsAtom},
                                  {user_who_performed_action, ActingUser}]),
                    ok
             end,
    case value_global(KeyAsAtom) of
        not_found ->
            {error_string, "Parameter does not exist"};
        _         ->
            record_clear(KeyAsAtom),
            case is_transaction() of
                true  -> Notify;
                false -> Notify()
            end
    end.

clear_component(Component, ActingUser) ->
    case list_component(Component) of
        [] ->
            ok;
        Xs ->
            [clear(pget(vhost, X),
                   pget(component, X),
                   pget(name, X),
                   ActingUser) || X <- Xs],
            ok
    end.

-spec clear_any(rabbit_types:vhost(), binary(), binary(), rabbit_types:username())
                     -> ok_thunk_or_error_string().

clear_any(VHost, Component, Name, ActingUser) ->
    Notify = fun () ->
                     case lookup_component(Component) of
                         {ok, Mod} -> event_notify(
                                        parameter_cleared, VHost, Component,
                                        [{name, Name},
                                         {user_who_performed_action, ActingUser}]),
                                      Mod:notify_clear(VHost, Component, Name, ActingUser);
                         _         -> ok
                     end
             end,
    case lookup(VHost, Component, Name) of
        not_found -> {error_string, "Parameter does not exist"};
        _         -> record_clear(VHost, Component, Name),
                     case is_transaction() of
                         true  -> Notify;
                         false -> Notify()
                     end
    end.

is_transaction() ->
    rabbit_khepri:try_mnesia_or_khepri(
      fun() -> mnesia:is_transaction() end,
      fun() -> khepri_tx:is_transaction() end).

record_clear(Key) ->
    rabbit_khepri:try_mnesia_or_khepri(
      fun() -> mnesia_clear(Key) end,
      fun() -> khepri_clear(Key) end).

record_clear(VHost, Comp, Name) ->
    rabbit_khepri:try_mnesia_or_khepri(
      fun() -> mnesia_clear(VHost, Comp, Name) end,
      fun() -> khepri_clear(VHost, Comp, Name) end).

mnesia_clear(Key) ->
    F = fun () ->
                ok = mnesia:delete(?TABLE, Key, write)
        end,
    ok = rabbit_misc:execute_mnesia_transaction(F).

mnesia_clear(VHost, Component, Name) ->
    F = fun () ->
                ok = mnesia:delete(?TABLE, {VHost, Component, Name}, write)
        end,
    ok = rabbit_misc:execute_mnesia_transaction(
           rabbit_vhost:with_in_mnesia(VHost, F)).

khepri_clear(Key) ->
    Path = khepri_rp_path(Key),
    F = fun () ->
                {ok, _} = khepri_tx:delete(Path),
                ok
        end,
    ok = rabbit_khepri:transaction(F).

khepri_clear(VHost, Component, Name) ->
    Path = khepri_rp_path({VHost, Component, Name}),
    F = fun () ->
                {ok, _} = khepri_tx:delete(Path),
                ok
        end,
    ok = rabbit_khepri:transaction(
           rabbit_vhost:with_in_khepri(VHost, F)).

event_notify(_Event, _VHost, <<"policy">>, _Props) ->
    ok;
event_notify(Event, none, Component, Props) ->
    rabbit_event:notify(Event, [{component, Component} | Props]);
event_notify(Event, VHost, Component, Props) ->
    rabbit_event:notify(Event, [{vhost,     VHost},
                                {component, Component} | Props]).

-spec list() -> [rabbit_types:infos()].

list() ->
    rabbit_khepri:try_mnesia_or_khepri(
      fun() -> list_in_mnesia() end,
      fun() -> list_in_khepri() end).

list_in_mnesia() ->
    [p(P) || #runtime_parameters{ key = {_VHost, Comp, _Name}} = P <-
             rabbit_misc:dirty_read_all(?TABLE), Comp /= <<"policy">>].

list_in_khepri() ->
    Path = khepri_vhost_rp_path(?STAR, ?STAR, ?STAR),
    {ok, Map} = rabbit_khepri:match_and_get_data(Path),
    [p(P) || #runtime_parameters{ key = {_VHost, Comp, _Name}} = P <- maps:values(Map),
             Comp /= <<"policy">>].

-spec list(rabbit_types:vhost() | '_') -> [rabbit_types:infos()].

list(VHost) -> list(VHost, '_').

-spec list_component(binary()) -> [rabbit_types:infos()].

list_component(Component) -> list('_',   Component).

%% Not dirty_match_object since that would not be transactional when used in a
%% tx context
-spec list(rabbit_types:vhost() | '_', binary() | '_')
                -> [rabbit_types:infos()].

list(VHost, Component) ->
    rabbit_khepri:try_mnesia_or_khepri(
      fun() -> list_in_mnesia(VHost, Component) end,
      fun() -> list_in_khepri(VHost, Component) end).

list_in_mnesia(VHost, Component) ->
    mnesia:async_dirty(
      fun () ->
              case VHost of
                  '_' -> ok;
                  _   -> rabbit_vhost:assert(VHost)
              end,
              Match = #runtime_parameters{key = {VHost, Component, '_'},
                                          _   = '_'},
              [p(P) || #runtime_parameters{key = {_VHost, Comp, _Name}} = P <-
                           mnesia:match_object(?TABLE, Match, read),
                       Comp =/= <<"policy">> orelse Component =:= <<"policy">>]
      end).

list_in_khepri('_', Component) ->
    list_in_khepri(?STAR, Component);
list_in_khepri(VHost, '_') ->
    list_in_khepri(VHost, ?STAR);
list_in_khepri(VHost, Component) ->
    Path = khepri_vhost_rp_path(VHost, Component, ?STAR),
    rabbit_khepri:transaction(
      fun() ->
              case VHost of
                  ?STAR -> ok;
                  %% Inside of a transaction, using `rabbit_vhost:exists` will cause
                  %% a deadlock and timeout on the transaction, as it uses `rabbit_khepri:exists`.
                  %% The `with` function uses the `khepri_tx` API instead
                  _     -> rabbit_vhost:with_in_khepri(VHost, fun() -> ok end)
              end,
              case khepri_tx:get(Path) of
                  {ok, Result} ->
              [p(P) || #{data := #runtime_parameters{key = {_VHost, Comp, _Name}}} = #{data := P} <-
                           maps:values(Result),
                       Comp =/= <<"policy">> orelse Component =:= <<"policy">>];
                  _ ->
                      []
              end
      end, ro).

list_global() ->
    %% list only atom keys
    rabbit_khepri:try_mnesia_or_khepri(
      fun() -> list_global_in_mnesia() end,
      fun() -> list_global_in_khepri() end).

list_global_in_mnesia() ->
    %% list only atom keys
    mnesia:async_dirty(
        fun () ->
            Match = #runtime_parameters{key = '_', _ = '_'},
            [p(P) || P <- mnesia:match_object(?TABLE, Match, read),
                is_atom(P#runtime_parameters.key)]
        end).

list_global_in_khepri() ->
    %% list only atom keys
    Path = khepri_global_rp_path(?STAR),
    rabbit_khepri:transaction(
        fun () ->
                case khepri_tx:get(Path) of
                    {ok, Result} ->
                        [p(P) || #{data := P} <- maps:values(Result),
                                 is_atom(P#runtime_parameters.key)];
                    _ ->
                        []
                end
        end, ro).

-spec list_formatted(rabbit_types:vhost()) -> [rabbit_types:infos()].

list_formatted(VHost) ->
    [ format_parameter(info_keys(), P) || P <- list(VHost) ].

format_parameter(InfoKeys, P) ->
    lists:foldr(fun
                    (value, Acc) ->
                        [{value, rabbit_json:encode(pget(value, P))} | Acc];
                    (Key, Acc)   ->
                        case lists:keyfind(Key, 1, P) of
                            false      -> Acc;
                            {Key, Val} -> [{Key, Val} | Acc]
                        end
                end,
                [], InfoKeys).

-spec list_formatted(rabbit_types:vhost(), reference(), pid()) -> 'ok'.

list_formatted(VHost, Ref, AggregatorPid) ->
    rabbit_control_misc:emitting_map(
      AggregatorPid, Ref,
      fun(P) -> format_parameter(info_keys(), P) end, list(VHost)).

list_global_formatted() ->
    [ format_parameter(global_info_keys(), P) || P <- list_global() ].

list_global_formatted(Ref, AggregatorPid) ->
    rabbit_control_misc:emitting_map(
        AggregatorPid, Ref,
        fun(P) -> format_parameter(global_info_keys(), P) end, list_global()).

-spec lookup(rabbit_types:vhost(), binary(), binary())
                  -> rabbit_types:infos() | 'not_found'.

lookup(VHost, Component, Name) ->
    case lookup0({VHost, Component, Name}, rabbit_misc:const(not_found)) of
        not_found -> not_found;
        Params    -> p(Params)
    end.

lookup_global(Name)  ->
    case lookup0(Name, rabbit_misc:const(not_found)) of
        not_found -> not_found;
        Params    -> p(Params)
    end.

-spec value(rabbit_types:vhost(), binary(), binary()) -> term().

value(VHost, Comp, Name) -> value0({VHost, Comp, Name}).

-spec value(rabbit_types:vhost(), binary(), binary(), term()) -> term().

value(VHost, Comp, Name, Def) -> value0({VHost, Comp, Name}, Def).

-spec value_global(atom()) -> term() | 'not_found'.

value_global(Key) ->
    value0(Key).

-spec value_global(atom(), term()) -> term().

value_global(Key, Default) ->
    value0(Key, Default).

value0(Key) ->
    case lookup0(Key, rabbit_misc:const(not_found)) of
        not_found -> not_found;
        Params    -> Params#runtime_parameters.value
    end.

value0(Key, Default) ->
    Params = lookup0(Key, fun () -> lookup_missing(Key, Default) end),
    Params#runtime_parameters.value.

lookup0(Key, DefaultFun) ->
    rabbit_khepri:try_mnesia_or_khepri(
      fun() -> lookup0_in_mnesia(Key, DefaultFun) end,
      fun() -> lookup0_in_khepri(Key, DefaultFun) end).

lookup0_in_mnesia(Key, DefaultFun) ->
    case mnesia:dirty_read(?TABLE, Key) of
        []  -> DefaultFun();
        [R] -> R
    end.

lookup0_in_khepri(Key, DefaultFun) ->
    Path = khepri_rp_path(Key),
    case rabbit_khepri:get(Path) of
        {ok, #{data := R}}           -> R;
        {error, {node_not_found, _}} -> DefaultFun()
    end.

lookup_missing(Key, Default) ->
    rabbit_khepri:try_mnesia_or_khepri(
      fun() -> lookup_missing_in_mnesia(Key, Default) end,
      fun() -> lookup_missing_in_khepri(Key, Default) end).

lookup_missing_in_mnesia(Key, Default) ->
    rabbit_misc:execute_mnesia_transaction(
      fun () ->
              case mnesia:read(?TABLE, Key, read) of
                  []  -> Record = c(Key, Default),
                         mnesia:write(?TABLE, Record, write),
                         Record;
                  [R] -> R
              end
      end).

lookup_missing_in_khepri(Key, Default) ->
    Path = khepri_rp_path(Key),
    rabbit_khepri:transaction(
      fun () ->
              case khepri_tx:get(Path) of
                  {ok, #{Path := #{data := R}}} ->
                      R;
                  {ok, _} ->
                      Record = c(Key, Default),
                      khepri_tx:put(Path, Record),
                      Record
              end
      end).

c(Key, Default) ->
    #runtime_parameters{key   = Key,
                        value = Default}.

p(#runtime_parameters{key = {VHost, Component, Name}, value = Value}) ->
    [{vhost,     VHost},
     {component, Component},
     {name,      Name},
     {value,     Value}];

p(#runtime_parameters{key = Key, value = Value}) when is_atom(Key) ->
    [{name,      Key},
     {value,     Value}].

khepri_rp_path() ->
    [?MODULE].

khepri_rp_path({VHost, Component, Name}) ->
    khepri_vhost_rp_path(VHost, Component, Name);
khepri_rp_path(Key) ->
    khepri_global_rp_path(Key).

khepri_global_rp_path(Key) ->
    [?MODULE, global, Key].

khepri_vhost_rp_path(VHost, Component, Name) ->
    [?MODULE, per_vhost, VHost, Component, Name].

clear_data_in_khepri() ->
    Path = khepri_rp_path(),
    case rabbit_khepri:delete(Path) of
        {ok, _} -> ok;
        Error -> throw(Error)
    end.

mnesia_write_to_khepri(
  #runtime_parameters{key = {VHost, Comp, Name}} = RuntimeParam) ->
    Path = khepri_vhost_rp_path(VHost, Comp, Name),
    case rabbit_khepri:put(Path, RuntimeParam) of
        {ok, _} -> ok;
        Error -> throw(Error)
    end;
mnesia_write_to_khepri(
  #runtime_parameters{key = Key} = RuntimeParam) ->
    Path = khepri_global_rp_path(Key),
    case rabbit_khepri:put(Path, RuntimeParam) of
        {ok, _} -> ok;
        Error -> throw(Error)
    end.

mnesia_delete_to_khepri(
  #runtime_parameters{key = {VHost, Comp, Name}}) ->
    Path = khepri_vhost_rp_path(VHost, Comp, Name),
    case rabbit_khepri:delete(Path) of
        {ok, _} -> ok;
        Error -> throw(Error)
    end;
mnesia_delete_to_khepri(
  #runtime_parameters{key = Key}) ->
    Path = khepri_global_rp_path(Key),
    case rabbit_khepri:delete(Path) of
        {ok, _} -> ok;
        Error -> throw(Error)
    end.

-spec info_keys() -> rabbit_types:info_keys().

info_keys() -> [component, name, value].

global_info_keys() -> [name, value].

%%---------------------------------------------------------------------------

lookup_component(Component) ->
    case rabbit_registry:lookup_module(
           runtime_parameter, rabbit_data_coercion:to_atom(Component)) of
        {error, not_found} -> {errors,
                               [{"component ~s not found", [Component]}]};
        {ok, Module}       -> {ok, Module}
    end.

flatten_errors(L) ->
    case [{F, A} || I <- lists:flatten([L]), {error, F, A} <- [I]] of
        [] -> ok;
        E  -> {errors, E}
    end.
