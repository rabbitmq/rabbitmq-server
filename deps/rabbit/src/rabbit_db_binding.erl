%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2023 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_db_binding).

-include_lib("khepri/include/khepri.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").

-export([exists/1,
         create/2,
         delete/2,
         get_all/0,
         get_all/1,
         get_all/3,
         get_all_for_source/1,
         get_all_for_destination/1,
         fold/2]).

%% Routing. These functions are in the hot code path
-export([match/2, match_routing_key/3]).

%% Exported to be used by various rabbit_db_* modules
-export([
         delete_for_destination_in_mnesia/2,
         delete_for_destination_in_khepri/2,
         delete_all_for_exchange_in_mnesia/3,
         delete_all_for_exchange_in_khepri/3,
         delete_transient_for_destination_in_mnesia/1,
         has_for_source_in_mnesia/1,
         has_for_source_in_khepri/1,
         match_source_and_destination_in_khepri_tx/2
        ]).

-export([
         khepri_route_path/1,
         khepri_routes_path/0,
         khepri_route_exchange_path/1
        ]).

%% Recovery is only needed for transient entities. Once mnesia is removed, these
%% functions can be deleted
-export([recover/0, recover/1]).

%% For testing
-export([clear/0]).

-define(MNESIA_TABLE, rabbit_route).
-define(MNESIA_DURABLE_TABLE, rabbit_durable_route).
-define(MNESIA_SEMI_DURABLE_TABLE, rabbit_semi_durable_route).
-define(MNESIA_REVERSE_TABLE, rabbit_reverse_route).
-define(MNESIA_INDEX_TABLE, rabbit_index_route).

%% -------------------------------------------------------------------
%% exists().
%% -------------------------------------------------------------------

-spec exists(Binding) -> Exists when
      Binding :: rabbit_types:binding(),
      Exists :: boolean().
%% @doc Indicates if the binding `Binding' exists.
%%
%% @returns true if the binding exists, false otherwise.
%%
%% @private

exists(Binding) ->
    rabbit_khepri:handle_fallback(
      #{mnesia => fun() -> exists_in_mnesia(Binding) end,
        khepri => fun() -> exists_in_khepri(Binding) end
       }).

exists_in_mnesia(Binding) ->
    binding_action_in_mnesia(
      Binding, fun (_Src, _Dst) ->
                       rabbit_misc:const(mnesia:read({?MNESIA_TABLE, Binding}) /= [])
               end, fun not_found_or_absent_errs_in_mnesia/1).

binding_action_in_mnesia(#binding{source      = SrcName,
                                  destination = DstName}, Fun, ErrFun) ->
    SrcTable = table_for_resource(SrcName),
    DstTable = table_for_resource(DstName),
    rabbit_mnesia:execute_mnesia_tx_with_tail(
      fun () ->
              case {mnesia:read({SrcTable, SrcName}),
                    mnesia:read({DstTable, DstName})} of
                  {[Src], [Dst]} -> Fun(Src, Dst);
                  {[],    [_]  } -> ErrFun([SrcName]);
                  {[_],   []   } -> ErrFun([DstName]);
                  {[],    []   } -> ErrFun([SrcName, DstName])
              end
      end).

table_for_resource(#resource{kind = exchange}) -> rabbit_exchange;
table_for_resource(#resource{kind = queue})    -> rabbit_queue.

not_found_or_absent_errs_in_mnesia(Names) ->
    Errs = [not_found_or_absent_in_mnesia(Name) || Name <- Names],
    rabbit_misc:const({error, {resources_missing, Errs}}).

exists_in_khepri(#binding{source = SrcName,
                          destination = DstName} = Binding) ->
    Path = khepri_route_path(Binding),
    case rabbit_khepri:transaction(
           fun () ->
                   case {lookup_resource_in_khepri_tx(SrcName),
                         lookup_resource_in_khepri_tx(DstName)} of
                       {[_Src], [_Dst]} ->
                           case khepri_tx:get(Path) of
                               {ok, Set} ->
                                   {ok, Set};
                               _ ->
                                   {ok, not_found}
                           end;
                       Errs ->
                           Errs
                   end
           end) of
        {ok, not_found} -> false;
        {ok, Set} -> sets:is_element(Binding, Set);
        Errs -> not_found_errs_in_khepri(not_found(Errs, SrcName, DstName))
    end.

lookup_resource_in_khepri_tx(#resource{kind = queue} = Name) ->
    rabbit_db_queue:get_in_khepri_tx(Name);
lookup_resource_in_khepri_tx(#resource{kind = exchange} = Name) ->
    rabbit_db_exchange:get_in_khepri_tx(Name).

not_found_errs_in_khepri(Names) ->
    Errs = [{not_found, Name} || Name <- Names],
    {error, {resources_missing, Errs}}.

not_found({[], [_]}, SrcName, _) ->
    [SrcName];
not_found({[_], []}, _, DstName) ->
    [DstName];
not_found({[], []}, SrcName, DstName) ->
    [SrcName, DstName].

%% -------------------------------------------------------------------
%% create().
%% -------------------------------------------------------------------

-spec create(Binding, ChecksFun) -> Ret when
      Binding :: rabbit_types:binding(),
      Src :: rabbit_types:binding_source(),
      Dst :: rabbit_types:binding_destination(),
      ChecksFun :: fun((Src, Dst) -> ok | {error, Reason :: any()}),
      Ret :: ok | {error, Reason :: any()}.
%% @doc Writes a binding if it doesn't exist already and passes the validation in
%% `ChecksFun' i.e. exclusive access
%%
%% @returns ok, or an error if the validation has failed.
%%
%% @private

create(Binding, ChecksFun) ->
    rabbit_khepri:handle_fallback(
      #{mnesia => fun() -> create_in_mnesia(Binding, ChecksFun) end,
        khepri => fun() -> create_in_khepri(Binding, ChecksFun) end
       }).

create_in_mnesia(Binding, ChecksFun) ->
    binding_action_in_mnesia(
      Binding,
      fun (Src, Dst) ->
              lock_resource(Src, read),
              lock_resource(Dst, read),
              case ChecksFun(Src, Dst) of
                  ok ->
                      BindingType = rabbit_binding:binding_type(Src, Dst),
                      case mnesia:read({?MNESIA_TABLE, Binding}) of
                          []  ->
                              ok = sync_route(#route{binding = Binding}, BindingType,
                                              should_index_table(Src), fun mnesia:write/3),
                              MaybeSerial = rabbit_exchange:serialise_events(Src),
                              Serial = serial_in_mnesia(MaybeSerial, Src),
                              fun () ->
                                      rabbit_exchange:callback(Src, add_binding, Serial, [Src, Binding])
                              end;
                          [_] -> fun () -> ok end
                      end;
                  {error, _} = Err ->
                      rabbit_misc:const(Err)
              end
      end, fun not_found_or_absent_errs_in_mnesia/1).

create_in_khepri(#binding{source = SrcName,
                               destination = DstName} = Binding, ChecksFun) ->
    case {lookup_resource(SrcName), lookup_resource(DstName)} of
        {[Src], [Dst]} ->
            case ChecksFun(Src, Dst) of
                ok ->
                    RoutePath = khepri_route_path(Binding),
                    MaybeSerial = rabbit_exchange:serialise_events(Src),
                    Serial = rabbit_khepri:transaction(
                               fun() ->
                                       ExchangePath = khepri_route_exchange_path(SrcName),
                                       ok = khepri_tx:put(ExchangePath, #{type => Src#exchange.type}),
                                       case khepri_tx:get(RoutePath) of
                                           {ok, Set} ->
                                               case sets:is_element(Binding, Set) of
                                                   true ->
                                                       already_exists;
                                                   false ->
                                                       ok = khepri_tx:put(RoutePath, sets:add_element(Binding, Set)),
                                                       serial_in_khepri(MaybeSerial, Src)
                                               end;
                                           _ ->
                                               ok = khepri_tx:put(RoutePath, sets:add_element(Binding, sets:new([{version, 2}]))),
                                               serial_in_khepri(MaybeSerial, Src)
                                       end
                               end, rw),
                    case Serial of
                        already_exists ->
                            ok;
                        {error, _} = Error ->
                            Error;
                        _ ->
                            rabbit_exchange:callback(Src, add_binding, Serial, [Src, Binding])
                    end;
                {error, _} = Err ->
                    Err
            end;
        Errs ->
            not_found_errs_in_khepri(not_found(Errs, SrcName, DstName))
    end.

lookup_resource(#resource{kind = queue} = Name) ->
    case rabbit_db_queue:get(Name) of
        {error, _} -> [];
        {ok, Q} -> [Q]
    end;
lookup_resource(#resource{kind = exchange} = Name) ->
    case rabbit_db_exchange:get(Name) of
        {ok, X} -> [X];
        _ -> []
    end.

serial_in_khepri(false, _) ->
    none;
serial_in_khepri(true, X) ->
    rabbit_db_exchange:next_serial_in_khepri_tx(X).

%% -------------------------------------------------------------------
%% delete().
%% -------------------------------------------------------------------

-spec delete(Binding, ChecksFun) -> Ret when
      Binding :: rabbit_types:binding(),
      Src :: rabbit_types:binding_source(),
      Dst :: rabbit_types:binding_destination(),
      ChecksFun :: fun((Src, Dst) -> ok | {error, Reason :: any()}),
      Ret :: ok | {ok, rabbit_binding:deletions()} | {error, Reason :: any()}.
%% @doc Deletes a binding record from the database if it passes the validation in
%% `ChecksFun'. It also triggers the deletion of auto-delete exchanges if needed.
%%
%% @private

delete(Binding, ChecksFun) ->
    rabbit_khepri:handle_fallback(
      #{mnesia => fun() -> delete_in_mnesia(Binding, ChecksFun) end,
        khepri => fun() -> delete_in_khepri(Binding, ChecksFun) end
       }).

delete_in_mnesia(Binding, ChecksFun) ->
    binding_action_in_mnesia(
      Binding,
      fun (Src, Dst) ->
              lock_resource(Src, read),
              lock_resource(Dst, read),
              case mnesia:read(?MNESIA_TABLE, Binding, write) of
                  [] -> case mnesia:read(?MNESIA_DURABLE_TABLE, Binding, write) of
                            [] -> rabbit_misc:const(ok);
                            %% We still delete the binding and run
                            %% all post-delete functions if there is only
                            %% a durable route in the database
                            _  -> delete_in_mnesia(Src, Dst, Binding)
                        end;
                  _  -> case ChecksFun(Src, Dst) of
                            ok               -> delete_in_mnesia(Src, Dst, Binding);
                            {error, _} = Err -> rabbit_misc:const(Err)
                        end
              end
      end, fun absent_errs_only_in_mnesia/1).

-spec delete_in_mnesia(Src, Dst, Binding) -> Ret when
      Src :: rabbit_types:exchange() | amqqueue:amqqueue(),
      Dst :: rabbit_types:exchange() | amqqueue:amqqueue(),
      Binding :: rabbit_types:binding(),
      Ret :: fun(() -> {ok, rabbit_binding:deletions()}).
delete_in_mnesia(Src, Dst, B) ->
    ok = sync_route(#route{binding = B}, rabbit_binding:binding_type(Src, Dst),
                    should_index_table(Src), fun delete/3),
    Deletions0 = maybe_auto_delete_exchange_in_mnesia(
                   B#binding.source, [B], rabbit_binding:new_deletions(), false),
    fun() -> {ok, rabbit_binding:process_deletions(Deletions0)} end.

absent_errs_only_in_mnesia(Names) ->
    Errs = [E || Name <- Names,
                 {absent, _Q, _Reason} = E <- [not_found_or_absent_in_mnesia(Name)]],
    rabbit_misc:const(case Errs of
                          [] -> ok;
                          _  -> {error, {resources_missing, Errs}}
                      end).

not_found_or_absent_in_mnesia(#resource{kind = exchange} = Name) ->
    {not_found, Name};
not_found_or_absent_in_mnesia(#resource{kind = queue}    = Name) ->
    %% NB: we assume that the caller has already performed a lookup on
    %% rabbit_queue and not found anything
    case rabbit_db_queue:get_durable_in_mnesia_tx(Name) of
        {error, not_found} -> {not_found, Name};
        {ok, Q} -> {absent, Q, nodedown}
    end.

delete_in_khepri(#binding{source = SrcName,
                          destination = DstName} = Binding, ChecksFun) ->
    Path = khepri_route_path(Binding),
    case rabbit_khepri:transaction(
           fun () ->
                   case {lookup_resource_in_khepri_tx(SrcName),
                         lookup_resource_in_khepri_tx(DstName)} of
                       {[Src], [Dst]} ->
                           case exists_in_khepri(Path, Binding) of
                               false ->
                                   ok;
                               true ->
                                   case ChecksFun(Src, Dst) of
                                       ok ->
                                           ok = delete_in_khepri(Binding),
                                           maybe_auto_delete_exchange_in_khepri(Binding#binding.source, [Binding], rabbit_binding:new_deletions(), false);
                                       {error, _} = Err ->
                                           Err
                                   end
                           end;
                       _Errs ->
                           %% No absent queues, always present on disk
                           ok
                   end
           end) of
        ok ->
            ok;
        {error, _} = Err ->
            Err;
        Deletions ->
            {ok, rabbit_binding:process_deletions(Deletions)}
    end.

exists_in_khepri(Path, Binding) ->
    case khepri_tx:get(Path) of
        {ok, Set} ->
            sets:is_element(Binding, Set);
        _ ->
            false
    end.

delete_in_khepri(Binding) ->
    Path = khepri_route_path(Binding),
    case khepri_tx:get(Path) of
        {ok, Set0} ->
            Set = sets:del_element(Binding, Set0),
            case sets:is_empty(Set) of
                true ->
                    ok = khepri_tx:delete(Path);
                false ->
                    ok = khepri_tx:put(Path, Set)
            end;
        _ ->
            ok
    end.

maybe_auto_delete_exchange_in_khepri(XName, Bindings, Deletions, OnlyDurable) ->
    {Entry, Deletions1} =
        case rabbit_db_exchange:maybe_auto_delete_in_khepri(XName, OnlyDurable) of
            {not_deleted, X} ->
                {{X, not_deleted, Bindings}, Deletions};
            {deleted, X, Deletions2} ->
                {{X, deleted, Bindings},
                 rabbit_binding:combine_deletions(Deletions, Deletions2)}
        end,
    rabbit_binding:add_deletion(XName, Entry, Deletions1).

%% -------------------------------------------------------------------
%% get_all().
%% -------------------------------------------------------------------

-spec get_all() -> [Binding] when
      Binding :: rabbit_types:binding().
%% @doc Returns all explicit binding records, the bindings explicitly added and not
%% automatically generated to the default exchange.
%%
%% @returns the list of binding records.
%%
%% @private

get_all() ->
    rabbit_khepri:handle_fallback(
      #{mnesia => fun() -> get_all_in_mnesia() end,
        khepri => fun() -> get_all_in_khepri() end
       }).

get_all_in_mnesia() ->
    mnesia:async_dirty(
      fun () ->
              AllRoutes = mnesia:dirty_match_object(?MNESIA_TABLE, #route{_ = '_'}),
              [B || #route{binding = B} <- AllRoutes]
      end).

get_all_in_khepri() ->
     [B || #route{binding = B} <- ets:tab2list(rabbit_khepri_bindings)].

-spec get_all(VHostName) -> [Binding] when
      VHostName :: vhost:name(),
      Binding :: rabbit_types:binding().
%% @doc Returns all binding records in the given virtual host.
%%
%% @returns the list of binding records.
%%
%% @private

get_all(VHost) ->
    rabbit_khepri:handle_fallback(
      #{mnesia => fun() -> get_all_in_mnesia(VHost) end,
        khepri => fun() -> get_all_in_khepri(VHost) end
       }).

get_all_in_mnesia(VHost) ->
    VHostResource = rabbit_misc:r(VHost, '_'),
    Match = #route{binding = #binding{source      = VHostResource,
                                      destination = VHostResource,
                                      _           = '_'},
                   _       = '_'},
    [B || #route{binding = B} <- rabbit_db:list_in_mnesia(?MNESIA_TABLE, Match)].

get_all_in_khepri(VHost) ->
    VHostResource = rabbit_misc:r(VHost, '_'),
    Match = #route{binding = #binding{source      = VHostResource,
                                      destination = VHostResource,
                                      _           = '_'}},
    [B || #route{binding = B} <- ets:match_object(rabbit_khepri_bindings, Match)].

-spec get_all(Src, Dst, Reverse) -> [Binding] when
      Src :: rabbit_types:binding_source(),
      Dst :: rabbit_types:binding_destination(),
      Reverse :: boolean(),
      Binding :: rabbit_types:binding().
%% @doc Returns all binding records for a given source and destination
%% in the given virtual host.
%%
%% @returns the list of binding records.
%%
%% @private

get_all(SrcName, DstName, Reverse) ->
    rabbit_khepri:handle_fallback(
      #{mnesia => fun() -> get_all_in_mnesia(SrcName, DstName, Reverse) end,
        khepri => fun() -> get_all_in_khepri(SrcName, DstName) end
       }).

get_all_in_mnesia(SrcName, DstName, Reverse) ->
    Route = #route{binding = #binding{source      = SrcName,
                                      destination = DstName,
                                      _           = '_'}},
    Fun = list_for_route(Route, Reverse),
    mnesia:async_dirty(Fun).

get_all_in_khepri(SrcName, DstName) ->
    MatchHead = #route{binding = #binding{source      = SrcName,
                                          destination = DstName,
                                          _           = '_'}},
    [B || #route{binding = B} <- ets:match_object(rabbit_khepri_bindings, MatchHead)].

%% -------------------------------------------------------------------
%% get_all_for_source().
%% -------------------------------------------------------------------

-spec get_all_for_source(Src) -> [Binding] when
      Src :: rabbit_types:binding_source(),
      Binding :: rabbit_types:binding().
%% @doc Returns all binding records for a given exchange in the given virtual host.
%%
%% @returns the list of binding records.
%%
%% @private

get_all_for_source(Resource) ->
    rabbit_khepri:handle_fallback(
      #{mnesia => fun() -> get_all_for_source_in_mnesia(Resource) end,
        khepri => fun() -> get_all_for_source_in_khepri(Resource) end
       }).

get_all_for_source_in_mnesia(Resource) ->
    Route = #route{binding = #binding{source = Resource, _ = '_'}},
    Fun = list_for_route(Route, false),
    mnesia:async_dirty(Fun).

list_for_route(Route, false) ->
    fun() ->
            [B || #route{binding = B} <- mnesia:match_object(?MNESIA_TABLE, Route, read)]
    end;
list_for_route(Route, true) ->
    fun() ->
            [rabbit_binding:reverse_binding(B) ||
             #reverse_route{reverse_binding = B} <-
             mnesia:match_object(?MNESIA_REVERSE_TABLE,
                                 rabbit_binding:reverse_route(Route), read)]
    end.

get_all_for_source_in_khepri(Resource) ->
    Route = #route{binding = #binding{source = Resource, _ = '_'}},
    [B || #route{binding = B} <- ets:match_object(rabbit_khepri_bindings, Route)].

%% -------------------------------------------------------------------
%% get_all_for_destination().
%% -------------------------------------------------------------------

-spec get_all_for_destination(Dst) -> [Binding] when
      Dst :: rabbit_types:binding_destination(),
      Binding :: rabbit_types:binding().
%% @doc Returns all binding records for a given exchange or queue destination
%%  in the given virtual host.
%%
%% @returns the list of binding records.
%%
%% @private

get_all_for_destination(Dst) ->
    rabbit_khepri:handle_fallback(
      #{mnesia => fun() -> get_all_for_destination_in_mnesia(Dst) end,
        khepri => fun() -> get_all_for_destination_in_khepri(Dst) end
       }).

get_all_for_destination_in_mnesia(Dst) ->
    Route = #route{binding = #binding{destination = Dst,
                                      _ = '_'}},
    Fun = list_for_route(Route, true),
    mnesia:async_dirty(Fun).

get_all_for_destination_in_khepri(Destination) ->
    Match = #route{binding = #binding{destination = Destination,
                                      _           = '_'}},
    [B || #route{binding = B} <- ets:match_object(rabbit_khepri_bindings, Match)].

%% -------------------------------------------------------------------
%% fold().
%% -------------------------------------------------------------------

-spec fold(Fun, Acc) -> Acc when
      Fun :: fun((Binding :: rabbit_types:binding(), Acc) -> Acc),
      Acc :: any().
%% @doc Folds over all the bindings, making it more efficient than `get_all()'
%% and folding over the returned binding list.
%%
%% Just used by prometheus_rabbitmq_core_metrics_collector to iterate over the
%% bindings.
%%
%% @returns the fold accumulator.
%%
%% @private

fold(Fun, Acc) ->
    rabbit_khepri:handle_fallback(
      #{mnesia => fun() -> fold_in_mnesia(Fun, Acc) end,
        khepri => fun() -> fold_in_khepri(Fun, Acc) end
       }).

fold_in_mnesia(Fun, Acc) ->
    ets:foldl(fun(#route{binding = Binding}, Acc0) ->
                      Fun(Binding, Acc0)
              end, Acc, ?MNESIA_TABLE).

fold_in_khepri(Fun, Acc) ->
    Path = khepri_routes_path() ++ [_VHost = ?KHEPRI_WILDCARD_STAR,
                                    _SrcName = ?KHEPRI_WILDCARD_STAR,
                                    rabbit_khepri:if_has_data_wildcard()],
    {ok, Res} = rabbit_khepri:fold(
                  Path,
                  fun(_, #{data := SetOfBindings}, Acc0) ->
                          lists:foldl(fun(Binding, Acc1) ->
                                              Fun(Binding, Acc1)
                                      end, Acc0, sets:to_list(SetOfBindings))
                  end, Acc),
    Res.

%% Routing - HOT CODE PATH
%% -------------------------------------------------------------------
%% match().
%% -------------------------------------------------------------------

-spec match(Src, MatchFun) -> [Dst] when
      Src :: rabbit_types:binding_source(),
      Dst :: rabbit_types:binding_destination(),
      Binding :: rabbit_types:binding(),
      MatchFun :: fun((Binding) -> boolean()).
%% @doc Matches all binding records that have `Src' as source of the binding
%% and for which `MatchFun' returns `true'.
%%
%% @returns the list of destinations
%%
%% @private

match(SrcName, Match) ->
    rabbit_khepri:handle_fallback(
      #{mnesia => fun() -> match_in_mnesia(SrcName, Match) end,
        khepri => fun() -> match_in_khepri(SrcName, Match) end
       }).

match_in_mnesia(SrcName, Match) ->
    MatchHead = #route{binding = #binding{source      = SrcName,
                                          _           = '_'}},
    Routes = ets:select(?MNESIA_TABLE, [{MatchHead, [], [['$_']]}]),
    [Dest || [#route{binding = Binding = #binding{destination = Dest}}] <-
                 Routes, Match(Binding)].

match_in_khepri(SrcName, Match) ->
    MatchHead = #route{binding = #binding{source      = SrcName,
                                          _           = '_'}},
    Routes = ets:select(rabbit_khepri_bindings, [{MatchHead, [], [['$_']]}]),
    [Dest || [#route{binding = Binding = #binding{destination = Dest}}] <-
                 Routes, Match(Binding)].

%% Routing - HOT CODE PATH
%% -------------------------------------------------------------------
%% match_routing_key().
%% -------------------------------------------------------------------

-spec match_routing_key(Src, RoutingKeys, UseIndex) -> [Dst] when
      Src :: rabbit_types:binding_source(),
      Dst :: rabbit_types:binding_destination(),
      RoutingKeys :: [binary() | '_'],
      UseIndex :: boolean().
%% @doc Matches all binding records that have `Src' as source of the binding
%% and that match any routing key in `RoutingKeys'.
%%
%% @returns the list of destinations
%%
%% @private

match_routing_key(SrcName, RoutingKeys, UseIndex) ->
    rabbit_khepri:handle_fallback(
      #{mnesia => fun() -> match_routing_key_in_mnesia(SrcName, RoutingKeys, UseIndex) end,
        khepri => fun() -> match_routing_key_in_khepri(SrcName, RoutingKeys) end
       }).

match_routing_key_in_mnesia(SrcName, RoutingKeys, UseIndex) ->
    case UseIndex of
        true ->
            route_v2(?MNESIA_INDEX_TABLE, SrcName, RoutingKeys);
        _ ->
            route_in_mnesia_v1(SrcName, RoutingKeys)
    end.

match_routing_key_in_khepri(Src, ['_']) ->
    MatchHead = #index_route{source_key  = {Src, '_'},
                             destination = '$1',
                             _           = '_'},
    ets:select(rabbit_khepri_index_route, [{MatchHead, [], ['$1']}]);

match_routing_key_in_khepri(Src, RoutingKeys) ->
    lists:foldl(
      fun(RK, Acc) ->
              try
                  Dst = ets:lookup_element(
                          rabbit_khepri_index_route,
                          {Src, RK},
                          #index_route.destination),
                  Dst ++ Acc
              catch
                  _:_:_ -> Acc
              end
      end, [], RoutingKeys).

%% -------------------------------------------------------------------
%% recover().
%% -------------------------------------------------------------------

-spec recover() -> ok.
%% @doc Recovers all durable routes
%%
%% @private

recover() ->
    rabbit_khepri:handle_fallback(
      #{mnesia => fun() -> recover_in_mnesia() end,
        %% Nothing to do in khepri, single table storage
        khepri => ok
       }).

recover_in_mnesia() ->
    rabbit_mnesia:execute_mnesia_transaction(
      fun () ->
              _ = mnesia:lock({table, ?MNESIA_DURABLE_TABLE}, read),
              _ = mnesia:lock({table, ?MNESIA_SEMI_DURABLE_TABLE}, write),
              Routes = rabbit_mnesia:dirty_read_all(?MNESIA_DURABLE_TABLE),
              Fun = fun(Route) ->
                            mnesia:dirty_write(?MNESIA_SEMI_DURABLE_TABLE, Route)
                    end,
              lists:foreach(Fun, Routes),
              ok
    end).

-spec recover(RecoverFun) -> ok when
      Route :: #route{},
      Src :: rabbit_types:binding_source(),
      Dst :: rabbit_types:binding_destination(),
      Binding :: rabbit_types:binding(),
      Exchange :: rabbit_types:exchange(),
      RecoverFun :: fun((Route, Src, Dst, fun((Binding, Exchange) -> ok)) -> ok).
%% @doc Recovers all semi-durable routes
%%
%% @private

recover(RecoverFun) ->
    rabbit_khepri:handle_fallback(
      #{mnesia => fun() -> recover_in_mnesia(RecoverFun) end,
        khepri => ok
       }).

recover_in_mnesia(RecoverFun) ->
    _ = [RecoverFun(Route, Src, Dst, fun recover_semi_durable_route/2) ||
         #route{binding = #binding{destination = Dst,
                                   source = Src}} = Route <-
         rabbit_mnesia:dirty_read_all(?MNESIA_SEMI_DURABLE_TABLE)],
    ok.

%% -------------------------------------------------------------------
%% delete_all_for_exchange_in_mnesia().
%% -------------------------------------------------------------------

-spec delete_all_for_exchange_in_mnesia(Exchange, OnlyDurable, RemoveBindingsForSource)
                                       -> Ret when
      Exchange :: rabbit_types:exchange(),
      OnlyDurable :: boolean(),
      RemoveBindingsForSource :: boolean(),
      Binding :: rabbit_types:binding(),
      Ret :: {deleted, Exchange, [Binding], rabbit_binding:deletions()}.

delete_all_for_exchange_in_mnesia(X = #exchange{name = XName}, OnlyDurable, RemoveBindingsForSource) ->
    Bindings = case RemoveBindingsForSource of
                   true  -> delete_for_source_in_mnesia(XName);
                   false -> []
               end,
    {deleted, X, Bindings, delete_for_destination_in_mnesia(XName, OnlyDurable, fun delete_routes/1)}.

delete_for_source_in_mnesia(#exchange{name = SrcName} = SrcX) ->
    delete_for_source_in_mnesia(SrcName, should_index_table(SrcX));
delete_for_source_in_mnesia(SrcName) ->
    delete_for_source_in_mnesia(SrcName, undefined).

-spec delete_for_source_in_mnesia(rabbit_types:binding_source(),
                                           boolean() | undefined) -> [rabbit_types:binding()].
delete_for_source_in_mnesia(SrcName, ShouldIndexTable) ->
    lock_resource(SrcName),
    Match = #route{binding = #binding{source = SrcName, _ = '_'}},
    delete_routes(
      lists:usort(
        mnesia:dirty_match_object(?MNESIA_TABLE, Match) ++
            mnesia:dirty_match_object(?MNESIA_SEMI_DURABLE_TABLE, Match)),
      ShouldIndexTable).

%% -------------------------------------------------------------------
%% delete_all_for_exchange_in_khepri().
%% -------------------------------------------------------------------

-spec delete_all_for_exchange_in_khepri(Exchange, OnlyDurable, RemoveBindingsForSource)
                                       -> Ret when
      Exchange :: rabbit_types:exchange(),
      OnlyDurable :: boolean(),
      RemoveBindingsForSource :: boolean(),
      Binding :: rabbit_types:binding(),
      Ret :: {deleted, Exchange, [Binding], rabbit_binding:deletions()}.

delete_all_for_exchange_in_khepri(X = #exchange{name = XName}, OnlyDurable, RemoveBindingsForSource) ->
    Bindings = case RemoveBindingsForSource of
                   true  -> delete_for_source_in_khepri(XName);
                   false -> []
               end,
    {deleted, X, Bindings, delete_for_destination_in_khepri(XName, OnlyDurable)}.

delete_for_source_in_khepri(#resource{virtual_host = VHost, name = Name}) ->
    Path = khepri_routes_path() ++ [VHost, Name],
    {ok, Bindings} = khepri_tx:get_many(Path ++ [rabbit_khepri:if_has_data_wildcard()]),
    ok = khepri_tx:delete(Path),
    maps:fold(fun(_P, Set, Acc) ->
                      sets:to_list(Set) ++ Acc
              end, [], Bindings).

%% -------------------------------------------------------------------
%% delete_for_destination_in_mnesia().
%% -------------------------------------------------------------------

-spec delete_for_destination_in_mnesia(Dst, OnlyDurable) -> Deletions when
      Dst :: rabbit_types:binding_destination(),
      OnlyDurable :: boolean(),
      Deletions :: rabbit_binding:deletions().

delete_for_destination_in_mnesia(DstName, OnlyDurable) ->
    delete_for_destination_in_mnesia(DstName, OnlyDurable, fun delete_routes/1).

delete_for_destination_in_mnesia(DstName, OnlyDurable, Fun) ->
    lock_resource(DstName),
    MatchFwd = #route{binding = #binding{destination = DstName, _ = '_'}},
    MatchRev = rabbit_binding:reverse_route(MatchFwd),
    Routes = case OnlyDurable of
                 false ->
                        [rabbit_binding:reverse_route(R) ||
                              R <- mnesia:dirty_match_object(
                                    ?MNESIA_REVERSE_TABLE, MatchRev)];
                 true  -> lists:usort(
                            mnesia:dirty_match_object(
                              ?MNESIA_DURABLE_TABLE, MatchFwd) ++
                                mnesia:dirty_match_object(
                                  ?MNESIA_SEMI_DURABLE_TABLE, MatchFwd))
             end,
    Bindings = Fun(Routes),
    rabbit_binding:group_bindings_fold(fun maybe_auto_delete_exchange_in_mnesia/4,
                                       lists:keysort(#binding.source, Bindings), OnlyDurable).

%% -------------------------------------------------------------------
%% delete_for_destination_in_khepri().
%% -------------------------------------------------------------------

-spec delete_for_destination_in_khepri(Dst, OnlyDurable) -> Deletions when
      Dst :: rabbit_types:binding_destination(),
      OnlyDurable :: boolean(),
      Deletions :: rabbit_binding:deletions().

delete_for_destination_in_khepri(DstName, OnlyDurable) ->
    BindingsMap = match_destination_in_khepri(DstName),
    maps:foreach(fun(K, _V) -> khepri_tx:delete(K) end, BindingsMap),
    Bindings = maps:fold(fun(_, Set, Acc) ->
                                 sets:to_list(Set) ++ Acc
                         end, [], BindingsMap),
    rabbit_binding:group_bindings_fold(fun maybe_auto_delete_exchange_in_khepri/4,
                                       lists:keysort(#binding.source, Bindings), OnlyDurable).

match_destination_in_khepri(#resource{virtual_host = VHost, kind = Kind, name = Name}) ->
    Path = khepri_routes_path() ++ [VHost, ?KHEPRI_WILDCARD_STAR, Kind, Name, ?KHEPRI_WILDCARD_STAR_STAR],
    {ok, Map} = khepri_tx:get_many(Path),
    Map.

%% -------------------------------------------------------------------
%% delete_transient_for_destination_in_mnesia().
%% -------------------------------------------------------------------

-spec delete_transient_for_destination_in_mnesia(rabbit_types:binding_destination()) -> rabbit_binding:deletions().
delete_transient_for_destination_in_mnesia(DstName) ->
    delete_for_destination_in_mnesia(DstName, false, fun delete_transient_routes/1).

delete_transient_routes(Routes) ->
    lists:map(fun(#route{binding = #binding{source = Src} = Binding} = Route) ->
                      {ok, X} = rabbit_db_exchange:get(Src),
                      ok = sync_transient_route(Route, should_index_table(X), fun delete/3),
                      Binding
              end, Routes).

%% -------------------------------------------------------------------
%% has_for_source_in_mnesia().
%% -------------------------------------------------------------------

-spec has_for_source_in_mnesia(rabbit_types:binding_source()) -> boolean().

has_for_source_in_mnesia(SrcName) ->
    Match = #route{binding = #binding{source = SrcName, _ = '_'}},
    %% we need to check for semi-durable routes (which subsumes
    %% durable routes) here too in case a bunch of routes to durable
    %% queues have been removed temporarily as a result of a node
    %% failure
    contains(?MNESIA_TABLE, Match) orelse
        contains(?MNESIA_SEMI_DURABLE_TABLE, Match).

%% -------------------------------------------------------------------
%% has_for_source_in_khepri().
%% -------------------------------------------------------------------

-spec has_for_source_in_khepri(rabbit_types:binding_source()) -> boolean().

has_for_source_in_khepri(#resource{virtual_host = VHost, name = Name}) ->
    Path = khepri_routes_path() ++ [VHost, Name, rabbit_khepri:if_has_data_wildcard()],
    case khepri_tx:get_many(Path) of
        {ok, Map} ->
            maps:size(Map) > 0;
        _ ->
            false
    end.

%% -------------------------------------------------------------------
%% match_source_and_destination_in_khepri_tx().
%% -------------------------------------------------------------------

-spec match_source_and_destination_in_khepri_tx(Src, Dst) -> Bindings when
      Src :: rabbit_types:binding_source(),
      Dst :: rabbit_types:binding_destination(),
      Bindings :: [Binding :: rabbit_types:binding()].

match_source_and_destination_in_khepri_tx(#resource{virtual_host = VHost, name = Name},
                                          #resource{kind = Kind, name = DstName}) ->
    Path = khepri_routes_path() ++ [VHost, Name, Kind, DstName, rabbit_khepri:if_has_data_wildcard()],
    case khepri_tx:get_many(Path) of
        {ok, Map} -> maps:values(Map);
        _         -> []
    end.

%% -------------------------------------------------------------------
%% clear().
%% -------------------------------------------------------------------

-spec clear() -> ok.
%% @doc Deletes all bindings.
%%
%% @private

clear() ->
    rabbit_khepri:handle_fallback(
      #{mnesia => fun() -> clear_in_mnesia() end,
        khepri => fun() -> clear_in_khepri() end}).

clear_in_mnesia() ->
    {atomic, ok} = mnesia:clear_table(?MNESIA_TABLE),
    {atomic, ok} = mnesia:clear_table(?MNESIA_DURABLE_TABLE),
    {atomic, ok} = mnesia:clear_table(?MNESIA_SEMI_DURABLE_TABLE),
    {atomic, ok} = mnesia:clear_table(?MNESIA_REVERSE_TABLE),
    {atomic, ok} = mnesia:clear_table(?MNESIA_INDEX_TABLE),
    ok.

clear_in_khepri() ->
    Path = rabbit_db_binding:khepri_routes_path(),
    case rabbit_khepri:delete(Path) of
        ok -> ok;
        Error -> throw(Error)
    end.

%% --------------------------------------------------------------
%% Paths
%% --------------------------------------------------------------
khepri_route_path(#binding{source = #resource{virtual_host = VHost, name = SrcName},
                           destination = #resource{kind = Kind, name = DstName},
                           key = RoutingKey}) ->
    [?MODULE, routes, VHost, SrcName, Kind, DstName, RoutingKey].

khepri_routes_path() ->
    [?MODULE, routes].

khepri_route_exchange_path(#resource{virtual_host = VHost, name = SrcName}) ->
    [?MODULE, routes, VHost, SrcName].

%% --------------------------------------------------------------
%% Internal
%% --------------------------------------------------------------
delete_routes(Routes) ->
    delete_routes(Routes, undefined).

delete_routes(Routes, ShouldIndexTable) ->
    %% This partitioning allows us to suppress unnecessary delete
    %% operations on disk tables, which require an fsync.
    {RamRoutes, DiskRoutes} =
        lists:partition(fun (R) -> mnesia:read(
                                     ?MNESIA_DURABLE_TABLE, R#route.binding, read) == [] end,
                        Routes),
    {RamOnlyRoutes, SemiDurableRoutes} =
        lists:partition(fun (R) -> mnesia:read(
                                     ?MNESIA_SEMI_DURABLE_TABLE, R#route.binding, read) == [] end,
                        RamRoutes),
    %% Of course the destination might not really be durable but it's
    %% just as easy to try to delete it from the semi-durable table
    %% than check first
    [ok = sync_route(R, durable, ShouldIndexTable, fun delete/3) ||
        R <- DiskRoutes],
    [ok = sync_route(R, semi_durable, ShouldIndexTable, fun delete/3) ||
        R <- SemiDurableRoutes],
    [ok = sync_route(R, transient, ShouldIndexTable, fun delete/3) ||
        R <- RamOnlyRoutes],
    _ = case ShouldIndexTable of
        B when is_boolean(B) ->
            ok;
        undefined ->
            [begin
                 case rabbit_db_exchange:get(Src) of
                     {ok, X} ->
                         ok = sync_index_route(R, should_index_table(X), fun delete/3);
                     _ ->
                         ok
                 end
             end || #route{binding = #binding{source = Src}} = R <- Routes]
    end,
    [R#route.binding || R <- Routes].

delete(Tab, #route{binding = B}, LockKind) ->
    mnesia:delete(Tab, B, LockKind);
delete(Tab, #reverse_route{reverse_binding = B}, LockKind) ->
    mnesia:delete(Tab, B, LockKind);
delete(Tab, #index_route{} = Record, LockKind) ->
    mnesia:delete_object(Tab, Record, LockKind).

%% Only the direct exchange type uses the rabbit_index_route table to store its
%% bindings by table key tuple {SourceExchange, RoutingKey}.
%% Other built-in exchange types lookup destinations by SourceExchange, and
%% therefore will not need to read from the rabbit_index_route index table.
%% Therefore, we avoid inserting and deleting into rabbit_index_route for other exchange
%% types. This reduces write lock conflicts on the same tuple {SourceExchange, RoutingKey}
%% reducing the number of restarted Mnesia transactions.
should_index_table(#exchange{name = #resource{name = Name},
                             type = direct})
  when Name =/= <<>> ->
    true;
should_index_table(_) ->
    false.

recover_semi_durable_route(#route{binding = B} = Route, X) ->
    MaybeSerial = rabbit_exchange:serialise_events(X),
    rabbit_mnesia:execute_mnesia_transaction(
      fun () ->
              case mnesia:read(?MNESIA_SEMI_DURABLE_TABLE, B, read) of
                  [] -> no_recover;
                  _  -> ok = sync_transient_route(Route, should_index_table(X), fun mnesia:write/3),
                        serial_in_mnesia(MaybeSerial, X)
              end
      end,
      fun (no_recover, _) -> ok;
          (_Serial, true) -> rabbit_exchange:callback(X, add_binding, transaction, [X, B]);
          (Serial, false) -> rabbit_exchange:callback(X, add_binding, Serial, [X, B])
      end).

serial_in_mnesia(false, _) ->
    none;
serial_in_mnesia(true, X) ->
    rabbit_db_exchange:next_serial_in_mnesia_tx(X#exchange.name).

sync_route(Route, durable, ShouldIndexTable, Fun) ->
    ok = Fun(?MNESIA_DURABLE_TABLE, Route, write),
    sync_route(Route, semi_durable, ShouldIndexTable, Fun);

sync_route(Route, semi_durable, ShouldIndexTable, Fun) ->
    ok = Fun(?MNESIA_SEMI_DURABLE_TABLE, Route, write),
    sync_route(Route, transient, ShouldIndexTable, Fun);

sync_route(Route, transient, ShouldIndexTable, Fun) ->
    sync_transient_route(Route, ShouldIndexTable, Fun).

sync_transient_route(Route, ShouldIndexTable, Fun) ->
    ok = Fun(?MNESIA_TABLE, Route, write),
    ok = Fun(?MNESIA_REVERSE_TABLE, rabbit_binding:reverse_route(Route), write),
    ok = sync_index_route(Route, ShouldIndexTable, Fun).

sync_index_route(Route, true, Fun) ->
    ok = Fun(?MNESIA_INDEX_TABLE, rabbit_binding:index_route(Route), write);
sync_index_route(_, _, _) ->
    ok.

-spec maybe_auto_delete_exchange_in_mnesia(ExchangeName, [Binding], Deletions, OnlyDurable)
                                          -> Ret when
      ExchangeName :: rabbit_exchange:name(),
      Binding :: rabbit_types:binding(),
      Deletions :: rabbit_binding:deletions(),
      OnlyDurable :: boolean(),
      Ret :: rabbit_binding:deletions().
maybe_auto_delete_exchange_in_mnesia(XName, Bindings, Deletions, OnlyDurable) ->
    {Entry, Deletions1} =
        case rabbit_db_exchange:maybe_auto_delete_in_mnesia(XName, OnlyDurable) of
            {not_deleted, X} ->
                {{X, not_deleted, Bindings}, Deletions};
            {deleted, X, Deletions2} ->
                {{X, deleted, Bindings},
                 rabbit_binding:combine_deletions(Deletions, Deletions2)}
        end,
    rabbit_binding:add_deletion(XName, Entry, Deletions1).

%% Instead of locking entire table on remove operations we can lock the
%% affected resource only.
lock_resource(Name) -> lock_resource(Name, write).

lock_resource(Name, LockKind) ->
    _ = mnesia:lock({global, Name, mnesia:table_info(?MNESIA_TABLE, where_to_write)},
                    LockKind),
    ok.

contains(Table, MatchHead) ->
    continue(mnesia:select(Table, [{MatchHead, [], ['$_']}], 1, read)).

continue('$end_of_table')    -> false;
continue({[_|_], _})         -> true;
continue({[], Continuation}) -> continue(mnesia:select(Continuation)).

%% Routing. Hot code path
%% -------------------------------------------------------------------------
route_in_mnesia_v1(SrcName, [RoutingKey]) ->
    MatchHead = #route{binding = #binding{source      = SrcName,
                                          destination = '$1',
                                          key         = RoutingKey,
                                          _           = '_'}},
    ets:select(?MNESIA_TABLE, [{MatchHead, [], ['$1']}]);
route_in_mnesia_v1(SrcName, [_|_] = RoutingKeys) ->
    %% Normally we'd call mnesia:dirty_select/2 here, but that is quite
    %% expensive for the same reasons as above, and, additionally, due to
    %% mnesia 'fixing' the table with ets:safe_fixtable/2, which is wholly
    %% unnecessary. According to the ets docs (and the code in erl_db.c),
    %% 'select' is safe anyway ("Functions that internally traverse over a
    %% table, like select and match, will give the same guarantee as
    %% safe_fixtable.") and, furthermore, even the lower level iterators
    %% ('first' and 'next') are safe on ordered_set tables ("Note that for
    %% tables of the ordered_set type, safe_fixtable/2 is not necessary as
    %% calls to first/1 and next/2 will always succeed."), which
    %% rabbit_route is.
    MatchHead = #route{binding = #binding{source      = SrcName,
                                          destination = '$1',
                                          key         = '$2',
                                          _           = '_'}},
    Conditions = [list_to_tuple(['orelse' | [{'=:=', '$2', RKey} ||
                                                RKey <- RoutingKeys]])],
    ets:select(?MNESIA_TABLE, [{MatchHead, Conditions, ['$1']}]).

%% rabbit_router:match_routing_key/2 uses ets:select/2 to get destinations.
%% ets:select/2 is expensive because it needs to compile the match spec every
%% time and lookup does not happen by a hash key.
%%
%% In contrast, route_v2/2 increases end-to-end message sending throughput
%% (i.e. from RabbitMQ client to the queue process) by up to 35% by using ets:lookup_element/3.
%% Only the direct exchange type uses the rabbit_index_route table to store its
%% bindings by table key tuple {SourceExchange, RoutingKey}.
-spec route_v2(ets:table(), rabbit_types:binding_source(), [rabbit_router:routing_key(), ...]) ->
    rabbit_router:match_result().
route_v2(Table, SrcName, [RoutingKey]) ->
    %% optimization
    destinations(Table, SrcName, RoutingKey);
route_v2(Table, SrcName, [_|_] = RoutingKeys) ->
    lists:flatmap(fun(Key) ->
                          destinations(Table, SrcName, Key)
                  end, RoutingKeys).

destinations(Table, SrcName, RoutingKey) ->
    %% Prefer try-catch block over checking Key existence with ets:member/2.
    %% The latter reduces throughput by a few thousand messages per second because
    %% of function db_member_hash in file erl_db_hash.c.
    %% We optimise for the happy path, that is the binding / table key is present.
    try
        ets:lookup_element(Table,
                           {SrcName, RoutingKey},
                           #index_route.destination)
    catch
        error:badarg ->
            []
    end.

