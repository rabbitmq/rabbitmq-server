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

-module(rabbit_binding).
-include("rabbit.hrl").

-export([recover/0, exists/1, add/1, remove/1, add/2, remove/2, list/1]).
-export([list_for_source/1, list_for_destination/1,
         list_for_source_and_destination/2]).
-export([info_keys/0, info/1, info/2, info_all/1, info_all/2]).
%% these must all be run inside a mnesia tx
-export([has_for_source/1, remove_for_source/1,
         remove_for_destination/1, remove_transient_for_destination/1]).

%%----------------------------------------------------------------------------

-ifdef(use_specs).

-export_type([key/0]).

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

-spec(recover/0 :: () -> [rabbit_types:binding()]).
-spec(exists/1 :: (rabbit_types:binding()) -> boolean() | bind_errors()).
-spec(add/1 :: (rabbit_types:binding()) -> bind_res()).
-spec(remove/1 :: (rabbit_types:binding()) ->
                       bind_res() | rabbit_types:error('binding_not_found')).
-spec(add/2 :: (rabbit_types:binding(), inner_fun()) -> bind_res()).
-spec(remove/2 :: (rabbit_types:binding(), inner_fun()) ->
                       bind_res() | rabbit_types:error('binding_not_found')).
-spec(list/1 :: (rabbit_types:vhost()) -> bindings()).
-spec(list_for_source/1 ::
        (rabbit_types:binding_source()) -> bindings()).
-spec(list_for_destination/1 ::
        (rabbit_types:binding_destination()) -> bindings()).
-spec(list_for_source_and_destination/2 ::
        (rabbit_types:binding_source(), rabbit_types:binding_destination()) ->
                                                bindings()).
-spec(info_keys/0 :: () -> [rabbit_types:info_key()]).
-spec(info/1 :: (rabbit_types:binding()) -> [rabbit_types:info()]).
-spec(info/2 :: (rabbit_types:binding(), [rabbit_types:info_key()]) ->
                     [rabbit_types:info()]).
-spec(info_all/1 :: (rabbit_types:vhost()) -> [[rabbit_types:info()]]).
-spec(info_all/2 ::(rabbit_types:vhost(), [rabbit_types:info_key()])
                    -> [[rabbit_types:info()]]).
-spec(has_for_source/1 :: (rabbit_types:binding_source()) -> boolean()).
-spec(remove_for_source/1 :: (rabbit_types:binding_source()) -> bindings()).
-spec(remove_for_destination/1 ::
        (rabbit_types:binding_destination()) -> fun (() -> any())).
-spec(remove_transient_for_destination/1 ::
        (rabbit_types:binding_destination()) -> fun (() -> any())).

-endif.

%%----------------------------------------------------------------------------

-define(INFO_KEYS, [source_name, source_kind,
                    destination_name, destination_kind,
                    routing_key, arguments]).

recover() ->
    rabbit_misc:table_fold(
      fun (Route = #route{binding = B}, Acc) ->
              {_, ReverseRoute} = route_with_reverse(Route),
              ok = mnesia:write(rabbit_route, Route, write),
              ok = mnesia:write(rabbit_reverse_route, ReverseRoute, write),
              [B | Acc]
      end, [], rabbit_durable_route).

exists(Binding) ->
    binding_action(
      Binding,
      fun (_Src, _Dst, B) -> mnesia:read({rabbit_route, B}) /= [] end).

add(Binding) -> add(Binding, fun (_Src, _Dst) -> ok end).

remove(Binding) -> remove(Binding, fun (_Src, _Dst) -> ok end).

add(Binding, InnerFun) ->
    case binding_action(
           Binding,
           fun (Src, Dst, B) ->
                   %% this argument is used to check queue exclusivity;
                   %% in general, we want to fail on that in preference to
                   %% anything else
                   case InnerFun(Src, Dst) of
                       ok ->
                           case mnesia:read({rabbit_route, B}) of
                               []  -> ok = sync_binding(
                                             B, all_durable([Src, Dst]),
                                             fun mnesia:write/3),
                                      {new, Src, B};
                               [_] -> {existing, Src, B}
                           end;
                       {error, _} = E ->
                           E
                   end
           end) of
        {new, Src = #exchange{ type = Type }, B} ->
            ok = (type_to_module(Type)):add_binding(Src, B),
            rabbit_event:notify(binding_created, info(B));
        {existing, _, _} ->
            ok;
        {error, _} = Err ->
            Err
    end.

remove(Binding, InnerFun) ->
    case binding_action(
           Binding,
           fun (Src, Dst, B) ->
                   case mnesia:match_object(rabbit_route, #route{binding = B},
                                            write) of
                       [] ->
                           {error, binding_not_found};
                       [_] ->
                           case InnerFun(Src, Dst) of
                               ok ->
                                   ok = sync_binding(
                                          B, all_durable([Src, Dst]),
                                          fun mnesia:delete_object/3),
                                   Deleted =
                                       rabbit_exchange:maybe_auto_delete(Src),
                                   {{Deleted, Src}, B};
                               {error, _} = E ->
                                   E
                           end
                   end
           end) of
        {error, _} = Err ->
            Err;
        {{IsDeleted, Src}, B} ->
            ok = post_binding_removal(IsDeleted, Src, [B]),
            rabbit_event:notify(binding_deleted, info(B)),
            ok
    end.

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

sync_binding(Binding, Durable, Fun) ->
    ok = case Durable of
             true  -> Fun(rabbit_durable_route,
                          #route{binding = Binding}, write);
             false -> ok
         end,
    {Route, ReverseRoute} = route_with_reverse(Binding),
    ok = Fun(rabbit_route, Route, write),
    ok = Fun(rabbit_reverse_route, ReverseRoute, write),
    ok.

call_with_source_and_destination(SrcName, DstName, Fun) ->
    SrcTable = table_for_resource(SrcName),
    DstTable = table_for_resource(DstName),
    rabbit_misc:execute_mnesia_transaction(
      fun () -> case {mnesia:read({SrcTable, SrcName}),
                      mnesia:read({DstTable, DstName})} of
                    {[Src], [Dst]} -> Fun(Src, Dst);
                    {[],    [_]  } -> {error, source_not_found};
                    {[_],   []   } -> {error, destination_not_found};
                    {[],    []   } -> {error, source_and_destination_not_found}
                end
      end).

table_for_resource(#resource{kind = exchange}) -> rabbit_exchange;
table_for_resource(#resource{kind = queue})    -> rabbit_queue.

%% Used with atoms from records; e.g., the type is expected to exist.
type_to_module(T) ->
    {ok, Module} = rabbit_exchange_type_registry:lookup_module(T),
    Module.

contains(Table, MatchHead) ->
    continue(mnesia:select(Table, [{MatchHead, [], ['$_']}], 1, read)).

continue('$end_of_table')    -> false;
continue({[_|_], _})         -> true;
continue({[], Continuation}) -> continue(mnesia:select(Continuation)).

remove_for_destination(DstName, FwdDeleteFun) ->
    DeletedBindings =
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
    Grouped = group_bindings_and_auto_delete(
                lists:keysort(#binding.source, DeletedBindings), []),
    fun () ->
            lists:foreach(
              fun ({{IsDeleted, Src}, Bs}) ->
                      ok = post_binding_removal(IsDeleted, Src, Bs)
              end, Grouped)
    end.

post_binding_removal(IsDeleted, Src = #exchange{ type = Type }, Bs) ->
    Module = type_to_module(Type),
    case IsDeleted of
        {auto_deleted, Fun} -> ok = Module:delete(Src, Bs),
                               Fun(),
                               ok;
        not_deleted         -> ok = Module:remove_bindings(Src, Bs)
    end.

%% Requires that its input binding list is sorted in exchange-name
%% order, so that the grouping of bindings (for passing to
%% group_bindings_and_auto_delete1) works properly.
group_bindings_and_auto_delete([], Acc) ->
    Acc;
group_bindings_and_auto_delete(
  [B = #binding{source = SrcName} | Bs], Acc) ->
    group_bindings_and_auto_delete(SrcName, Bs, [B], Acc).

group_bindings_and_auto_delete(
  SrcName, [B = #binding{source = SrcName} | Bs], Bindings, Acc) ->
    group_bindings_and_auto_delete(SrcName, Bs, [B | Bindings], Acc);
group_bindings_and_auto_delete(SrcName, Removed, Bindings, Acc) ->
    %% either Removed is [], or its head has a non-matching SrcName
    [Src] = mnesia:read({rabbit_exchange, SrcName}),
    NewAcc = [{{rabbit_exchange:maybe_auto_delete(Src), Src}, Bindings} | Acc],
    group_bindings_and_auto_delete(Removed, NewAcc).

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
