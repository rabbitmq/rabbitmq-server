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

-module(rabbit_binding).
-include("rabbit.hrl").

-export([recover/2, exists/1, add/1, add/2, remove/1, remove/2, list/1]).
-export([list_for_source/1, list_for_destination/1,
         list_for_source_and_destination/2]).
-export([new_deletions/0, combine_deletions/2, add_deletion/3,
         process_deletions/1]).
-export([info_keys/0, info/1, info/2, info_all/1, info_all/2]).
%% these must all be run inside a mnesia tx
-export([has_for_source/1, remove_for_source/1,
         remove_for_destination/1, remove_transient_for_destination/1]).

%%----------------------------------------------------------------------------

-ifdef(use_specs).

-export_type([key/0, deletions/0]).

-type(key() :: binary()).

-type(bind_errors() :: rabbit_types:error('source_not_found' |
                                          'destination_not_found' |
                                          'source_and_destination_not_found')).
-type(bind_ok_or_error() :: 'ok' | bind_errors() |
                            rabbit_types:error('binding_not_found')).
-type(bind_res() :: bind_ok_or_error() | rabbit_misc:thunk(bind_ok_or_error())).
-type(inner_fun() ::
        fun((rabbit_types:exchange(),
             rabbit_types:exchange() | rabbit_types:amqqueue()) ->
                   rabbit_types:ok_or_error(rabbit_types:amqp_error()))).
-type(bindings() :: [rabbit_types:binding()]).

-opaque(deletions() :: dict()).

-spec(recover/2 :: ([rabbit_exchange:name()], [rabbit_amqqueue:name()]) ->
                        'ok').
-spec(exists/1 :: (rabbit_types:binding()) -> boolean() | bind_errors()).
-spec(add/1    :: (rabbit_types:binding())              -> bind_res()).
-spec(add/2    :: (rabbit_types:binding(), inner_fun()) -> bind_res()).
-spec(remove/1 :: (rabbit_types:binding())              -> bind_res()).
-spec(remove/2 :: (rabbit_types:binding(), inner_fun()) -> bind_res()).
-spec(list/1 :: (rabbit_types:vhost()) -> bindings()).
-spec(list_for_source/1 ::
        (rabbit_types:binding_source()) -> bindings()).
-spec(list_for_destination/1 ::
        (rabbit_types:binding_destination()) -> bindings()).
-spec(list_for_source_and_destination/2 ::
        (rabbit_types:binding_source(), rabbit_types:binding_destination()) ->
                                                bindings()).
-spec(info_keys/0 :: () -> rabbit_types:info_keys()).
-spec(info/1 :: (rabbit_types:binding()) -> rabbit_types:infos()).
-spec(info/2 :: (rabbit_types:binding(), rabbit_types:info_keys()) ->
                     rabbit_types:infos()).
-spec(info_all/1 :: (rabbit_types:vhost()) -> [rabbit_types:infos()]).
-spec(info_all/2 ::(rabbit_types:vhost(), rabbit_types:info_keys())
                   -> [rabbit_types:infos()]).
-spec(has_for_source/1 :: (rabbit_types:binding_source()) -> boolean()).
-spec(remove_for_source/1 :: (rabbit_types:binding_source()) -> bindings()).
-spec(remove_for_destination/1 ::
        (rabbit_types:binding_destination()) -> deletions()).
-spec(remove_transient_for_destination/1 ::
        (rabbit_types:binding_destination()) -> deletions()).
-spec(process_deletions/1 :: (deletions()) -> rabbit_misc:thunk('ok')).
-spec(combine_deletions/2 :: (deletions(), deletions()) -> deletions()).
-spec(add_deletion/3 :: (rabbit_exchange:name(),
                         {'undefined' | rabbit_types:exchange(),
                          'deleted' | 'not_deleted',
                          bindings()}, deletions()) -> deletions()).
-spec(new_deletions/0 :: () -> deletions()).

-endif.

%%----------------------------------------------------------------------------

-define(INFO_KEYS, [source_name, source_kind,
                    destination_name, destination_kind,
                    routing_key, arguments]).

recover(XNames, QNames) ->
    rabbit_misc:table_filter(
      fun (Route) ->
              mnesia:read({rabbit_semi_durable_route, Route}) =:= []
      end,
      fun (Route,  true) ->
              ok = mnesia:write(rabbit_semi_durable_route, Route, write);
          (_Route, false) ->
              ok
      end, rabbit_durable_route),
    XNameSet = sets:from_list(XNames),
    QNameSet = sets:from_list(QNames),
    SelectSet = fun (#resource{kind = exchange}) -> XNameSet;
                    (#resource{kind = queue})    -> QNameSet
                end,
    {ok, Gatherer} = gatherer:start_link(),
    [recover_semi_durable_route(Gatherer, R, SelectSet(Dst)) ||
        R = #route{binding = #binding{destination = Dst}} <-
            rabbit_misc:dirty_read_all(rabbit_semi_durable_route)],
    empty = gatherer:out(Gatherer),
    ok = gatherer:stop(Gatherer),
    ok.

recover_semi_durable_route(Gatherer, R = #route{binding = B}, ToRecover) ->
    #binding{source = Src, destination = Dst} = B,
    case sets:is_element(Dst, ToRecover) of
        true  -> {ok, X} = rabbit_exchange:lookup(Src),
                 ok = gatherer:fork(Gatherer),
                 ok = worker_pool:submit_async(
                        fun () ->
                                recover_semi_durable_route_txn(R, X),
                                gatherer:finish(Gatherer)
                        end);
        false -> ok
    end.

recover_semi_durable_route_txn(R = #route{binding = B}, X) ->
    rabbit_misc:execute_mnesia_transaction(
      fun () ->
              case mnesia:match_object(rabbit_semi_durable_route, R, read) of
                  [] -> no_recover;
                  _  -> ok = sync_transient_route(R, fun mnesia:write/3),
                        rabbit_exchange:serial(X)
              end
      end,
      fun (no_recover, _)     -> ok;
          (_Serial,    true)  -> x_callback(transaction, X, add_binding, B);
          (Serial,     false) -> x_callback(Serial,      X, add_binding, B)
      end).

exists(Binding) ->
    binding_action(
      Binding, fun (_Src, _Dst, B) ->
                       rabbit_misc:const(mnesia:read({rabbit_route, B}) /= [])
               end).

add(Binding) -> add(Binding, fun (_Src, _Dst) -> ok end).

add(Binding, InnerFun) ->
    binding_action(
      Binding,
      fun (Src, Dst, B) ->
              %% this argument is used to check queue exclusivity;
              %% in general, we want to fail on that in preference to
              %% anything else
              case InnerFun(Src, Dst) of
                  ok               -> case mnesia:read({rabbit_route, B}) of
                                          []  -> add(Src, Dst, B);
                                          [_] -> fun rabbit_misc:const_ok/0
                                      end;
                  {error, _} = Err -> rabbit_misc:const(Err)
              end
      end).

add(Src, Dst, B) ->
    [SrcDurable, DstDurable] = [durable(E) || E <- [Src, Dst]],
    case (not (SrcDurable andalso DstDurable) orelse
          mnesia:read({rabbit_durable_route, B}) =:= []) of
        true  -> ok = sync_route(#route{binding = B}, SrcDurable, DstDurable,
                                 fun mnesia:write/3),
                 ok = rabbit_exchange:callback(
                        Src, add_binding, [transaction, Src, B]),
                 Serial = rabbit_exchange:serial(Src),
                 fun () ->
                     ok = rabbit_exchange:callback(
                            Src, add_binding, [Serial, Src, B]),
                     ok = rabbit_event:notify(binding_created, info(B))
                 end;
        false -> rabbit_misc:const({error, binding_not_found})
    end.

remove(Binding) -> remove(Binding, fun (_Src, _Dst) -> ok end).

remove(Binding, InnerFun) ->
    binding_action(
      Binding,
      fun (Src, Dst, B) ->
              case mnesia:read(rabbit_route, B, write) of
                  []  -> rabbit_misc:const({error, binding_not_found});
                  [_] -> case InnerFun(Src, Dst) of
                             ok               -> remove(Src, Dst, B);
                             {error, _} = Err -> rabbit_misc:const(Err)
                         end
              end
      end).

remove(Src, Dst, B) ->
    ok = sync_route(#route{binding = B}, durable(Src), durable(Dst),
                    fun mnesia:delete_object/3),
    Deletions = maybe_auto_delete(B#binding.source, [B], new_deletions()),
    process_deletions(Deletions).

list(VHostPath) ->
    VHostResource = rabbit_misc:r(VHostPath, '_'),
    Route = #route{binding = #binding{source      = VHostResource,
                                      destination = VHostResource,
                                      _           = '_'},
                   _       = '_'},
    [B || #route{binding = B} <- mnesia:dirty_match_object(rabbit_route,
                                                           Route)].

list_for_source(SrcName) ->
    mnesia:async_dirty(
      fun() ->
              Route = #route{binding = #binding{source = SrcName, _ = '_'}},
              [B || #route{binding = B}
                        <- mnesia:match_object(rabbit_route, Route, read)]
      end).

list_for_destination(DstName) ->
    mnesia:async_dirty(
      fun() ->
              Route = #route{binding = #binding{destination = DstName,
                                                _ = '_'}},
              [reverse_binding(B) ||
                  #reverse_route{reverse_binding = B} <-
                      mnesia:match_object(rabbit_reverse_route,
                                          reverse_route(Route), read)]
      end).

list_for_source_and_destination(SrcName, DstName) ->
    mnesia:async_dirty(
      fun() ->
              Route = #route{binding = #binding{source      = SrcName,
                                                destination = DstName,
                                                _           = '_'}},
              [B || #route{binding = B} <- mnesia:match_object(rabbit_route,
                                                               Route, read)]
      end).

info_keys() -> ?INFO_KEYS.

map(VHostPath, F) ->
    %% TODO: there is scope for optimisation here, e.g. using a
    %% cursor, parallelising the function invocation
    lists:map(F, list(VHostPath)).

infos(Items, B) -> [{Item, i(Item, B)} || Item <- Items].

i(source_name,      #binding{source      = SrcName})    -> SrcName#resource.name;
i(source_kind,      #binding{source      = SrcName})    -> SrcName#resource.kind;
i(destination_name, #binding{destination = DstName})    -> DstName#resource.name;
i(destination_kind, #binding{destination = DstName})    -> DstName#resource.kind;
i(routing_key,      #binding{key         = RoutingKey}) -> RoutingKey;
i(arguments,        #binding{args        = Arguments})  -> Arguments;
i(Item, _) -> throw({bad_argument, Item}).

info(B = #binding{}) -> infos(?INFO_KEYS, B).

info(B = #binding{}, Items) -> infos(Items, B).

info_all(VHostPath) -> map(VHostPath, fun (B) -> info(B) end).

info_all(VHostPath, Items) -> map(VHostPath, fun (B) -> info(B, Items) end).

has_for_source(SrcName) ->
    Match = #route{binding = #binding{source = SrcName, _ = '_'}},
    %% we need to check for durable routes here too in case a bunch of
    %% routes to durable queues have been removed temporarily as a
    %% result of a node failure
    contains(rabbit_route, Match) orelse
        contains(rabbit_semi_durable_route, Match).

remove_for_source(SrcName) ->
    lock_route_tables(),
    Match = #route{binding = #binding{source = SrcName, _ = '_'}},
    Routes = lists:usort(
               mnesia:match_object(rabbit_route, Match, write) ++
                   mnesia:match_object(rabbit_durable_route, Match, write)),
    [begin
         sync_route(Route, fun mnesia:delete_object/3),
         Route#route.binding
     end || Route <- Routes].

remove_for_destination(Dst) ->
    remove_for_destination(
      Dst, fun (R) -> sync_route(R, fun mnesia:delete_object/3) end).

remove_transient_for_destination(Dst) ->
    remove_for_destination(
      Dst, fun (R) -> sync_transient_route(R, fun mnesia:delete_object/3) end).

%%----------------------------------------------------------------------------

durable(#exchange{durable = D}) -> D;
durable(#amqqueue{durable = D}) -> D.

binding_action(Binding = #binding{source      = SrcName,
                                  destination = DstName,
                                  args        = Arguments}, Fun) ->
    call_with_source_and_destination(
      SrcName, DstName,
      fun (Src, Dst) ->
              SortedArgs = rabbit_misc:sort_field_table(Arguments),
              Fun(Src, Dst, Binding#binding{args = SortedArgs})
      end).

sync_route(R, Fun) -> sync_route(R, true, true, Fun).

sync_route(Route, true, true, Fun) ->
    ok = Fun(rabbit_durable_route, Route, write),
    sync_route(Route, false, true, Fun);

sync_route(Route, false, true, Fun) ->
    ok = Fun(rabbit_semi_durable_route, Route, write),
    sync_route(Route, false, false, Fun);

sync_route(Route, _SrcDurable, false, Fun) ->
    sync_transient_route(Route, Fun).

sync_transient_route(Route, Fun) ->
    ok = Fun(rabbit_route, Route, write),
    ok = Fun(rabbit_reverse_route, reverse_route(Route), write).

call_with_source_and_destination(SrcName, DstName, Fun) ->
    SrcTable = table_for_resource(SrcName),
    DstTable = table_for_resource(DstName),
    ErrFun = fun (Err) -> rabbit_misc:const({error, Err}) end,
    rabbit_misc:execute_mnesia_tx_with_tail(
      fun () ->
              case {mnesia:read({SrcTable, SrcName}),
                    mnesia:read({DstTable, DstName})} of
                  {[Src], [Dst]} -> Fun(Src, Dst);
                  {[],    [_]  } -> ErrFun(source_not_found);
                  {[_],   []   } -> ErrFun(destination_not_found);
                  {[],    []   } -> ErrFun(source_and_destination_not_found)
               end
      end).

table_for_resource(#resource{kind = exchange}) -> rabbit_exchange;
table_for_resource(#resource{kind = queue})    -> rabbit_queue.

contains(Table, MatchHead) ->
    continue(mnesia:select(Table, [{MatchHead, [], ['$_']}], 1, read)).

continue('$end_of_table')    -> false;
continue({[_|_], _})         -> true;
continue({[], Continuation}) -> continue(mnesia:select(Continuation)).

%% For bulk operations we lock the tables we are operating on in order
%% to reduce the time complexity. Without the table locks we end up
%% with num_tables*num_bulk_bindings row-level locks. Taking each lock
%% takes time proportional to the number of existing locks, thus
%% resulting in O(num_bulk_bindings^2) complexity.
%%
%% The locks need to be write locks since ultimately we end up
%% removing all these rows.
%%
%% The downside of all this is that no other binding operations except
%% lookup/routing (which uses dirty ops) can take place
%% concurrently. However, that is the case already since the bulk
%% operations involve mnesia:match_object calls with a partial key,
%% which entails taking a table lock.
lock_route_tables() ->
    [mnesia:lock({table, T}, write) || T <- [rabbit_route,
                                             rabbit_reverse_route,
                                             rabbit_semi_durable_route,
                                             rabbit_durable_route]].

remove_for_destination(DstName, DeleteFun) ->
    lock_route_tables(),
    Match = reverse_route(
              #route{binding = #binding{destination = DstName, _ = '_'}}),
    ReverseRoutes = mnesia:match_object(rabbit_reverse_route, Match, write),
    Bindings = [begin
                    Route = reverse_route(ReverseRoute),
                    ok = DeleteFun(Route),
                    Route#route.binding
                end || ReverseRoute <- ReverseRoutes],
    group_bindings_fold(fun maybe_auto_delete/3, new_deletions(),
                        lists:keysort(#binding.source, Bindings)).

%% Requires that its input binding list is sorted in exchange-name
%% order, so that the grouping of bindings (for passing to
%% group_bindings_and_auto_delete1) works properly.
group_bindings_fold(_Fun, Acc, []) ->
    Acc;
group_bindings_fold(Fun, Acc, [B = #binding{source = SrcName} | Bs]) ->
    group_bindings_fold(Fun, SrcName, Acc, Bs, [B]).

group_bindings_fold(
  Fun, SrcName, Acc, [B = #binding{source = SrcName} | Bs], Bindings) ->
    group_bindings_fold(Fun, SrcName, Acc, Bs, [B | Bindings]);
group_bindings_fold(Fun, SrcName, Acc, Removed, Bindings) ->
    %% Either Removed is [], or its head has a non-matching SrcName.
    group_bindings_fold(Fun, Fun(SrcName, Bindings, Acc), Removed).

maybe_auto_delete(XName, Bindings, Deletions) ->
    {Entry, Deletions1} =
        case mnesia:read({rabbit_exchange, XName}) of
            []  -> {{undefined, not_deleted, Bindings}, Deletions};
            [X] -> case rabbit_exchange:maybe_auto_delete(X) of
                       not_deleted ->
                           {{X, not_deleted, Bindings}, Deletions};
                       {deleted, Deletions2} ->
                           {{X, deleted, Bindings},
                            combine_deletions(Deletions, Deletions2)}
                   end
        end,
    add_deletion(XName, Entry, Deletions1).

reverse_route(#route{binding = Binding}) ->
    #reverse_route{reverse_binding = reverse_binding(Binding)};

reverse_route(#reverse_route{reverse_binding = Binding}) ->
    #route{binding = reverse_binding(Binding)}.

reverse_binding(#reverse_binding{source      = SrcName,
                                 destination = DstName,
                                 key         = Key,
                                 args        = Args}) ->
    #binding{source      = SrcName,
             destination = DstName,
             key         = Key,
             args        = Args};

reverse_binding(#binding{source      = SrcName,
                         destination = DstName,
                         key         = Key,
                         args        = Args}) ->
    #reverse_binding{source      = SrcName,
                     destination = DstName,
                     key         = Key,
                     args        = Args}.

%% ----------------------------------------------------------------------------
%% Binding / exchange deletion abstraction API
%% ----------------------------------------------------------------------------

anything_but( NotThis, NotThis, NotThis) -> NotThis;
anything_but( NotThis, NotThis,    This) -> This;
anything_but( NotThis,    This, NotThis) -> This;
anything_but(_NotThis,    This,    This) -> This.

new_deletions() -> dict:new().

add_deletion(XName, Entry, Deletions) ->
    dict:update(XName, fun (Entry1) -> merge_entry(Entry1, Entry) end,
                Entry, Deletions).

combine_deletions(Deletions1, Deletions2) ->
    dict:merge(fun (_XName, Entry1, Entry2) -> merge_entry(Entry1, Entry2) end,
               Deletions1, Deletions2).

merge_entry({X1, Deleted1, Bindings1}, {X2, Deleted2, Bindings2}) ->
    {anything_but(undefined, X1, X2),
     anything_but(not_deleted, Deleted1, Deleted2),
     [Bindings1 | Bindings2]}.

process_deletions(Deletions) ->
    AugmentedDeletions =
        dict:map(fun (_XName, {X, deleted, Bindings}) ->
                         Bs = lists:flatten(Bindings),
                         x_callback(transaction, X, delete, Bs),
                         {X, deleted, Bs, none};
                     (_XName, {X, not_deleted, Bindings}) ->
                         Bs = lists:flatten(Bindings),
                         x_callback(transaction, X, remove_bindings, Bs),
                         {X, not_deleted, Bs, rabbit_exchange:serial(X)}
                 end, Deletions),
    fun() ->
            dict:fold(fun (XName, {X, deleted, Bs, Serial}, ok) ->
                              ok = rabbit_event:notify(
                                     exchange_deleted, [{name, XName}]),
                              del_notify(Bs),
                              x_callback(Serial, X, delete, Bs);
                          (_XName, {X, not_deleted, Bs, Serial}, ok) ->
                              del_notify(Bs),
                              x_callback(Serial, X, remove_bindings, Bs)
                      end, ok, AugmentedDeletions)
    end.

del_notify(Bs) -> [rabbit_event:notify(binding_deleted, info(B)) || B <- Bs].

x_callback(Arg, X, F, Bs) -> ok = rabbit_exchange:callback(X, F, [Arg, X, Bs]).
