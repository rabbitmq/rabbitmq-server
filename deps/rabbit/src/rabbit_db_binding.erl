%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_db_binding).

-include_lib("khepri/include/khepri.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").

-include("include/rabbit_khepri.hrl").

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
         delete_for_destination/1,
         delete_for_destination_in_khepri/2,
         delete_all_for_exchange_in_khepri/3,
         has_for_source_in_khepri/1,
         match_source_and_destination_in_khepri_tx/2
        ]).

-export([
         khepri_route_path/1, khepri_route_path/5,
         khepri_route_path_to_args/1
        ]).

%% For testing
-export([clear/0]).

-define(KHEPRI_BINDINGS_PROJECTION, rabbit_khepri_binding).
-define(KHEPRI_ROUTE_BY_SOURCE_KEY_PROJECTION, rabbit_khepri_route_by_source_key).
-define(KHEPRI_ROUTE_BY_SOURCE_PROJECTION, rabbit_khepri_route_by_source).

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

exists(#binding{source = SrcName,
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
           end, ro) of
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
      ChecksFun :: fun((Src, Dst) -> ok | {error, ChecksErrReason}),
      ChecksErrReason :: any(),
      Ret :: ok | {error, ChecksErrReason} | rabbit_khepri:timeout_error().
%% @doc Writes a binding if it doesn't exist already and passes the validation in
%% `ChecksFun' i.e. exclusive access
%%
%% @returns ok, or an error if the validation has failed.
%%
%% @private

create(#binding{source = SrcName,
                destination = DstName} = Binding, ChecksFun) ->
    case {lookup_resource(SrcName), lookup_resource(DstName)} of
        {[Src], [Dst]} ->
            case ChecksFun(Src, Dst) of
                ok ->
                    RoutePath = khepri_route_path(Binding),
                    MaybeSerial = rabbit_exchange:serialise_events(Src),
                    Serial = rabbit_khepri:transaction(
                               fun() ->
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
      ChecksFun :: fun((Src, Dst) -> ok | {error, ChecksErrReason}),
      ChecksErrReason :: any(),
      Ret :: ok |
             {ok, rabbit_binding:deletions()} |
             {error, ChecksErrReason} |
             rabbit_khepri:timeout_error().
%% @doc Deletes a binding record from the database if it passes the validation in
%% `ChecksFun'. It also triggers the deletion of auto-delete exchanges if needed.
%%
%% @private

delete(#binding{source = SrcName,
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
            ok = rabbit_binding:process_deletions(Deletions),
            {ok, Deletions}
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
    case rabbit_db_exchange:maybe_auto_delete_in_khepri(XName, OnlyDurable) of
        {not_deleted, undefined} ->
            Deletions;
        {not_deleted, X} ->
            rabbit_binding:add_deletion(
              XName, X, not_deleted, Bindings, Deletions);
        {deleted, X, Deletions1} ->
            Deletions2 = rabbit_binding:combine_deletions(
                           Deletions, Deletions1),
            rabbit_binding:add_deletion(
              XName, X, deleted, Bindings, Deletions2)
    end.

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
    try
        [B || #route{binding = B} <- ets:tab2list(?KHEPRI_BINDINGS_PROJECTION)]
    catch
        error:badarg ->
            []
    end.

-spec get_all(VHostName) -> [Binding] when
      VHostName :: vhost:name(),
      Binding :: rabbit_types:binding().
%% @doc Returns all binding records in the given virtual host.
%%
%% @returns the list of binding records.
%%
%% @private

get_all(VHost) ->
    try
        VHostResource = rabbit_misc:r(VHost, '_'),
        Match = #route{binding = #binding{source      = VHostResource,
                                          destination = VHostResource,
                                          _           = '_'}},
        [B || #route{binding = B} <- ets:match_object(
                                       ?KHEPRI_BINDINGS_PROJECTION, Match)]
    catch
        error:badarg ->
            []
    end.

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

get_all(SrcName, DstName, _Reverse) ->
    try
        MatchHead = #route{binding = #binding{source      = SrcName,
                                              destination = DstName,
                                              _           = '_'}},
        [B || #route{binding = B} <- ets:match_object(
                                       ?KHEPRI_BINDINGS_PROJECTION, MatchHead)]
    catch
        error:badarg ->
            []
    end.

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
    try
        Route = #route{binding = #binding{source = Resource, _ = '_'}},
        [B || #route{binding = B} <- ets:match_object(
                                       ?KHEPRI_BINDINGS_PROJECTION, Route)]
    catch
        error:badarg ->
            []
    end.

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
    try
        Match = #route{binding = #binding{destination = Dst,
                                          _           = '_'}},
        [B || #route{binding = B} <- ets:match_object(
                                       ?KHEPRI_BINDINGS_PROJECTION, Match)]
    catch
        error:badarg ->
            []
    end.

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
    Path = khepri_route_path(
             _VHost = ?KHEPRI_WILDCARD_STAR,
             _SrcName = ?KHEPRI_WILDCARD_STAR,
             _Kind = ?KHEPRI_WILDCARD_STAR,
             _DstName = ?KHEPRI_WILDCARD_STAR,
             _RoutingKey = #if_has_data{}),
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
    try
        MatchHead = #route{binding = #binding{source      = SrcName,
                                              _           = '_'}},
        Routes = ets:select(
                   ?KHEPRI_BINDINGS_PROJECTION, [{MatchHead, [], [['$_']]}]),
        [Dest || [#route{binding = Binding = #binding{destination = Dest}}] <-
                     Routes, Match(Binding)]
    catch
        error:badarg ->
            []
    end.

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

match_routing_key(Src, ['_'], _UseIndex) ->
    try
        ets:lookup_element(?KHEPRI_ROUTE_BY_SOURCE_PROJECTION,
                           Src,
                           #route_by_source.destination,
                           [])
    catch
        error:badarg ->
            []
    end;
match_routing_key(Src, RoutingKeys, _UseIndex) ->
    lists:foldl(
      fun(RK, Acc) ->
              try
                  Dst = ets:lookup_element(
                          ?KHEPRI_ROUTE_BY_SOURCE_KEY_PROJECTION,
                          {Src, RK},
                          #index_route.destination),
                  Dst ++ Acc
              catch
                  _:_:_ -> Acc
              end
      end, [], RoutingKeys).

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

delete_for_source_in_khepri(#resource{virtual_host = VHost, name = SrcName}) ->
    Pattern = khepri_route_path(
                VHost,
                SrcName,
                ?KHEPRI_WILDCARD_STAR, %% Kind
                ?KHEPRI_WILDCARD_STAR, %% DstName
                #if_has_data{}), %% RoutingKey
    {ok, Bindings} = khepri_tx_adv:delete_many(Pattern),
    maps:fold(
      fun(Path, Props, Acc) ->
              case {Path, Props} of
                  {?RABBITMQ_KHEPRI_ROUTE_PATH(
                     VHost, SrcName, _Kind, _Name, _RoutingKey),
                   #{data := Set}} ->
                      sets:to_list(Set) ++ Acc;
                  {_, _} ->
                      Acc
              end
      end, [], Bindings).

%% -------------------------------------------------------------------
%% delete_for_destination_in_khepri().
%% -------------------------------------------------------------------

-spec delete_for_destination(Dst) -> Ret when
      Dst :: rabbit_types:binding_destination(),
      Ret :: Deletions | Error,
      Deletions :: rabbit_binding:deletions(),
      Error :: {error, any()}.

delete_for_destination(Dst) ->
    rabbit_khepri:transaction(
      fun () ->
              delete_for_destination_in_khepri(Dst, false)
      end).

-spec delete_for_destination_in_khepri(Dst, OnlyDurable) -> Deletions when
      Dst :: rabbit_types:binding_destination(),
      OnlyDurable :: boolean(),
      Deletions :: rabbit_binding:deletions().

delete_for_destination_in_khepri(#resource{virtual_host = VHost, kind = Kind, name = Name}, OnlyDurable) ->
    Pattern = khepri_route_path(
                VHost,
                ?KHEPRI_WILDCARD_STAR, %% SrcName
                Kind,
                Name,
                ?KHEPRI_WILDCARD_STAR), %% RoutingKey
    {ok, BindingsMap} = khepri_tx_adv:delete_many(Pattern),
    Bindings = maps:fold(
                 fun(Path, Props, Acc) ->
                         case {Path, Props} of
                             {?RABBITMQ_KHEPRI_ROUTE_PATH(
                                VHost, _SrcName, Kind, Name, _RoutingKey),
                              #{data := Set}} ->
                                 sets:to_list(Set) ++ Acc;
                             {_, _} ->
                                 Acc
                         end
                 end, [], BindingsMap),
    rabbit_binding:group_bindings_fold(fun maybe_auto_delete_exchange_in_khepri/4,
                                       lists:keysort(#binding.source, Bindings), OnlyDurable).

%% -------------------------------------------------------------------
%% has_for_source_in_khepri().
%% -------------------------------------------------------------------

-spec has_for_source_in_khepri(rabbit_types:binding_source()) -> boolean().

has_for_source_in_khepri(#resource{virtual_host = VHost, name = Name}) ->
    Path = khepri_route_path(
             VHost,
             Name,
             _Kind = ?KHEPRI_WILDCARD_STAR,
             _DstName = ?KHEPRI_WILDCARD_STAR,
             _RoutingKey = #if_has_data{}),
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
    Path = khepri_route_path(
             VHost, Name, Kind, DstName, _RoutingKey = #if_has_data{}),
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
    Path = khepri_route_path(
             _VHost = ?KHEPRI_WILDCARD_STAR,
             _SrcName = ?KHEPRI_WILDCARD_STAR,
             _Kind = ?KHEPRI_WILDCARD_STAR,
             _DstName = ?KHEPRI_WILDCARD_STAR,
             _RoutingKey = ?KHEPRI_WILDCARD_STAR),
    case rabbit_khepri:delete(Path) of
        ok -> ok;
        Error -> throw(Error)
    end.

%% --------------------------------------------------------------
%% Paths
%% --------------------------------------------------------------

khepri_route_path(
  #binding{source = #resource{virtual_host = VHost,
                              kind = exchange,
                              name = SrcName},
           destination = #resource{virtual_host = VHost,
                                   kind = Kind,
                                   name = DstName},
           key = RoutingKey}) ->
    khepri_route_path(VHost, SrcName, Kind, DstName, RoutingKey).

khepri_route_path(VHost, SrcName, Kind, DstName, BindingKey)
  when ?IS_KHEPRI_PATH_CONDITION(Kind) andalso
       ?IS_KHEPRI_PATH_CONDITION(DstName) andalso
       ?IS_KHEPRI_PATH_CONDITION(BindingKey) ->
    ?RABBITMQ_KHEPRI_ROUTE_PATH(VHost, SrcName, Kind, DstName, BindingKey).

khepri_route_path_to_args(Path) ->
    Pattern = khepri_route_path(
                '$VHost', '$SrcName', '$Kind', '$DstName', '$RoutingKey'),
    khepri_route_path_to_args(Pattern, Path, #{}).

khepri_route_path_to_args([Var | Pattern], [Value | Path], Result)
  when Var =:= '$VHost' orelse
       Var =:= '$SrcName' orelse
       Var =:= '$Kind' orelse
       Var =:= '$DstName' orelse
       Var =:= '$RoutingKey' ->
    Result1 = Result#{Var => Value},
    khepri_route_path_to_args(Pattern, Path, Result1);
khepri_route_path_to_args([Comp | Pattern], [Comp | Path], Result) ->
    khepri_route_path_to_args(Pattern, Path, Result);
khepri_route_path_to_args(
  [], _,
  #{'$VHost' := VHost,
    '$SrcName' := SrcName,
    '$Kind' := Kind,
    '$DstName' := DstName,
    '$RoutingKey' := RoutingKey}) ->
    {VHost, SrcName, Kind, DstName, RoutingKey}.
