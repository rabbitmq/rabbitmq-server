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
%% Copyright (c) 2007-2011 VMware, Inc.  All rights reserved.
%%

-module(rabbit_binding).
-include("rabbit.hrl").

-export([recover/2, exists/1, add/1, remove/1, add/2, remove/2, list/1]).
-export([list_for_source/1, list_for_destination/1,
         list_for_source_and_destination/2]).
-export([new_deletions/0, combine_deletions/2, add_deletion/3,
         process_deletions/2]).
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
-type(bind_res() :: 'ok' | bind_errors()).
-type(inner_fun() ::
        fun((rabbit_types:exchange(),
             rabbit_types:exchange() | rabbit_types:amqqueue()) ->
                   rabbit_types:ok_or_error(rabbit_types:amqp_error()))).
-type(bindings() :: [rabbit_types:binding()]).
-type(add_res() :: bind_res() | rabbit_misc:const(bind_res())).
-type(bind_or_error() :: bind_res() | rabbit_types:error('binding_not_found')).
-type(remove_res() :: bind_or_error() | rabbit_misc:const(bind_or_error())).

-opaque(deletions() :: dict()).

-spec(recover/2 :: ([rabbit_exchange:name()], [rabbit_amqqueue:name()]) ->
                        'ok').
-spec(exists/1 :: (rabbit_types:binding()) -> boolean() | bind_errors()).
-spec(add/1 :: (rabbit_types:binding()) -> add_res()).
-spec(remove/1 :: (rabbit_types:binding()) -> remove_res()).
-spec(add/2 :: (rabbit_types:binding(), inner_fun()) -> add_res()).
-spec(remove/2 :: (rabbit_types:binding(), inner_fun()) -> remove_res()).
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
-spec(process_deletions/2 :: (deletions(), boolean()) -> 'ok').
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
    XNameSet = sets:from_list(XNames),
    QNameSet = sets:from_list(QNames),
    rabbit_misc:table_map(
      fun (Route = #route{binding = B =
                              #binding{destination = Dst =
                                           #resource{kind = Kind}}}) ->
              %% The check against rabbit_durable_route is in case it
              %% disappeared between getting the list and here
              case mnesia:read({rabbit_durable_route, B}) =/= [] andalso
                  sets:is_element(Dst, case Kind of
                                           exchange -> XNameSet;
                                           queue    -> QNameSet
                                       end) of
                  true  -> ok = sync_transient_binding(
                                  Route, fun mnesia:write/3),
                           B;
                  false -> none
              end
      end,
      fun (none, _Tx) ->
              none;
          (B = #binding{source = Src}, Tx) ->
              {ok, X} = rabbit_exchange:lookup(Src),
              rabbit_exchange:callback(X, add_bindings, [Tx, X, [B]]),
              B
      end,
      rabbit_durable_route),
    ok.

exists(Binding) ->
    binding_action(
      Binding, fun (_Src, _Dst, B) ->
                       rabbit_misc:const(mnesia:read({rabbit_route, B}) /= [])
               end).

add(Binding) -> add(Binding, fun (_Src, _Dst) -> ok end).

remove(Binding) -> remove(Binding, fun (_Src, _Dst) -> ok end).

add(Binding, InnerFun) ->
    binding_action(
      Binding,
      fun (Src, Dst, B) ->
              %% this argument is used to check queue exclusivity;
              %% in general, we want to fail on that in preference to
              %% anything else
              case InnerFun(Src, Dst) of
                  ok               -> add(Src, Dst, B);
                  {error, _} = Err -> rabbit_misc:const(Err)
              end
      end).

add(Src, Dst, B) ->
    case mnesia:read({rabbit_route, B}) of
        []  -> Durable = all_durable([Src, Dst]),
               case (not Durable orelse
                     mnesia:read({rabbit_durable_route, B}) =:= []) of
                   true  -> ok = sync_binding(B, Durable, fun mnesia:write/3),
                            fun (Tx) ->
                                    ok = rabbit_exchange:callback(
                                           Src, add_bindings, [Tx, Src, [B]]),
                                    rabbit_event:notify_if(
                                      not Tx, binding_created, info(B))
                            end;
                   false -> rabbit_misc:const(not_found)
               end;
        [_] -> fun rabbit_misc:const_ok/1
    end.

remove(Binding, InnerFun) ->
    binding_action(
      Binding,
      fun (Src, Dst, B) ->
              Result =
                  case mnesia:match_object(rabbit_route, #route{binding = B},
                                           write) of
                      [] ->
                          {error, binding_not_found};
                      [_] ->
                          case InnerFun(Src, Dst) of
                              ok ->
                                  ok = sync_binding(B, all_durable([Src, Dst]),
                                                    fun mnesia:delete_object/3),
                                  {ok, maybe_auto_delete(B#binding.source,
                                                         [B], new_deletions())};
                              {error, _} = E ->
                                  E
                          end
                  end,
              case Result of
                  {error, _} = Err ->
                      rabbit_misc:const(Err);
                  {ok, Deletions} ->
                      fun (Tx) -> ok = process_deletions(Deletions, Tx) end
              end
      end).

list(VHostPath) ->
    VHostResource = rabbit_misc:r(VHostPath, '_'),
    Route = #route{binding = #binding{source      = VHostResource,
                                      destination = VHostResource,
                                      _           = '_'},
                   _       = '_'},
    [B || #route{binding = B} <- mnesia:dirty_match_object(rabbit_route,
                                                           Route)].

list_for_source(SrcName) ->
    Route = #route{binding = #binding{source = SrcName, _ = '_'}},
    [B || #route{binding = B} <- mnesia:dirty_match_object(rabbit_route,
                                                           Route)].

list_for_destination(DstName) ->
    Route = #route{binding = #binding{destination = DstName, _ = '_'}},
    [reverse_binding(B) || #reverse_route{reverse_binding = B} <-
                               mnesia:dirty_match_object(rabbit_reverse_route,
                                                         reverse_route(Route))].

list_for_source_and_destination(SrcName, DstName) ->
    Route = #route{binding = #binding{source      = SrcName,
                                      destination = DstName,
                                      _           = '_'}},
    [B || #route{binding = B} <- mnesia:dirty_match_object(rabbit_route,
                                                           Route)].

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
    contains(rabbit_route, Match) orelse contains(rabbit_durable_route, Match).

remove_for_source(SrcName) ->
    [begin
         ok = mnesia:delete_object(rabbit_reverse_route,
                                   reverse_route(Route), write),
         ok = delete_forward_routes(Route),
         Route#route.binding
     end || Route <- mnesia:match_object(
                       rabbit_route,
                       #route{binding = #binding{source = SrcName,
                                                 _      = '_'}},
                       write)].

remove_for_destination(DstName) ->
    remove_for_destination(DstName, fun delete_forward_routes/1).

remove_transient_for_destination(DstName) ->
    remove_for_destination(DstName, fun delete_transient_forward_routes/1).

%%----------------------------------------------------------------------------

all_durable(Resources) ->
    lists:all(fun (#exchange{durable = D}) -> D;
                  (#amqqueue{durable = D}) -> D
              end, Resources).

binding_action(Binding = #binding{source      = SrcName,
                                  destination = DstName,
                                  args        = Arguments}, Fun) ->
    call_with_source_and_destination(
      SrcName, DstName,
      fun (Src, Dst) ->
              SortedArgs = rabbit_misc:sort_field_table(Arguments),
              Fun(Src, Dst, Binding#binding{args = SortedArgs})
      end).

sync_binding(Binding, true, Fun) ->
    ok = Fun(rabbit_durable_route, #route{binding = Binding}, write),
    ok = sync_transient_binding(Binding, Fun);

sync_binding(Binding, false, Fun) ->
    ok = sync_transient_binding(Binding, Fun).

sync_transient_binding(Binding, Fun) ->
    {Route, ReverseRoute} = route_with_reverse(Binding),
    ok = Fun(rabbit_route, Route, write),
    ok = Fun(rabbit_reverse_route, ReverseRoute, write).

call_with_source_and_destination(SrcName, DstName, Fun) ->
    SrcTable = table_for_resource(SrcName),
    DstTable = table_for_resource(DstName),
    ErrFun = fun (Err) -> rabbit_misc:const(Err) end,
    rabbit_misc:execute_mnesia_tx_with_tail(
      fun () ->
              case {mnesia:read({SrcTable, SrcName}),
                    mnesia:read({DstTable, DstName})} of
                  {[Src], [Dst]} -> Fun(Src, Dst);
                  {[],    [_]  } -> ErrFun({error, source_not_found});
                  {[_],   []   } -> ErrFun({error, destination_not_found});
                  {[],    []   } -> ErrFun({error,
                                            source_and_destination_not_found})
              end
      end).

table_for_resource(#resource{kind = exchange}) -> rabbit_exchange;
table_for_resource(#resource{kind = queue})    -> rabbit_queue.

contains(Table, MatchHead) ->
    continue(mnesia:select(Table, [{MatchHead, [], ['$_']}], 1, read)).

continue('$end_of_table')    -> false;
continue({[_|_], _})         -> true;
continue({[], Continuation}) -> continue(mnesia:select(Continuation)).

remove_for_destination(DstName, FwdDeleteFun) ->
    Bindings =
        [begin
             Route = reverse_route(ReverseRoute),
             ok = FwdDeleteFun(Route),
             ok = mnesia:delete_object(rabbit_reverse_route,
                                       ReverseRoute, write),
             Route#route.binding
         end || ReverseRoute
                    <- mnesia:match_object(
                         rabbit_reverse_route,
                         reverse_route(#route{
                                          binding = #binding{
                                            destination = DstName,
                                            _           = '_'}}),
                         write)],
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

delete_forward_routes(Route) ->
    ok = mnesia:delete_object(rabbit_route, Route, write),
    ok = mnesia:delete_object(rabbit_durable_route, Route, write).

delete_transient_forward_routes(Route) ->
    ok = mnesia:delete_object(rabbit_route, Route, write).

route_with_reverse(#route{binding = Binding}) ->
    route_with_reverse(Binding);
route_with_reverse(Binding = #binding{}) ->
    Route = #route{binding = Binding},
    {Route, reverse_route(Route)}.

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

process_deletions(Deletions, Tx) ->
    dict:fold(
      fun (_XName, {X, Deleted, Bindings}, ok) ->
              FlatBindings = lists:flatten(Bindings),
              [rabbit_event:notify_if(not Tx, binding_deleted, info(B)) ||
                  B <- FlatBindings],
              case Deleted of
                  not_deleted ->
                      rabbit_exchange:callback(X, remove_bindings,
                                               [Tx, X, FlatBindings]);
                  deleted ->
                      rabbit_event:notify_if(not Tx, exchange_deleted,
                                             [{name, X#exchange.name}]),
                      rabbit_exchange:callback(X, delete, [Tx, X, FlatBindings])
              end
      end, ok, Deletions).
