%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2023 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_binding).
-include_lib("rabbit_common/include/rabbit.hrl").
-include("amqqueue.hrl").

-export([recover/0, recover/2, exists/1, add/2, add/3, remove/2, remove/3]).
-export([list/1, list_for_source/1, list_for_destination/1,
         list_for_source_and_destination/2, list_for_source_and_destination/3,
         list_explicit/0]).
-export([new_deletions/0, combine_deletions/2, add_deletion/3,
         process_deletions/1, notify_deletions/2, group_bindings_fold/3]).
-export([info_keys/0, info/1, info/2, info_all/1, info_all/2, info_all/4]).

-export([reverse_binding/1]).
-export([new/4]).
-export([reverse_route/1, index_route/1]).
-export([binding_type/2]).

-define(DEFAULT_EXCHANGE(VHostPath), #resource{virtual_host = VHostPath,
                                               kind = exchange,
                                               name = <<>>}).

%%----------------------------------------------------------------------------

-export_type([key/0, deletions/0]).

-type key() :: binary().

-type bind_errors() :: rabbit_types:error(
                         {'resources_missing',
                          [{'not_found', (rabbit_types:binding_source() |
                                          rabbit_types:binding_destination())} |
                           {'absent', amqqueue:amqqueue(), Reason :: term()}]}).

-type bind_ok_or_error() :: 'ok' | bind_errors() |
                            rabbit_types:error({'binding_invalid', string(), [any()]}) |
                            %% inner_fun() result
                            rabbit_types:error(rabbit_types:amqp_error()).
-type bind_res() :: bind_ok_or_error() | rabbit_misc:thunk(bind_ok_or_error()).
-type inner_fun() ::
        fun((rabbit_types:exchange(),
             rabbit_types:exchange() | amqqueue:amqqueue()) ->
                   rabbit_types:ok_or_error(rabbit_types:amqp_error())).
-type bindings() :: [rabbit_types:binding()].

%% TODO this should really be opaque but that seems to confuse 17.1's
%% dialyzer into objecting to everything that uses it.
-type deletions() :: dict:dict().

%%----------------------------------------------------------------------------

-spec new(rabbit_types:binding_source(),
          key(),
          rabbit_types:binding_destination(),
          rabbit_framing:amqp_table()) ->
    rabbit_types:binding().

new(Src, RoutingKey, Dst, #{}) ->
    new(Src, RoutingKey, Dst, []);
new(Src, RoutingKey, Dst, Arguments) when is_map(Arguments) ->
    new(Src, RoutingKey, Dst, maps:to_list(Arguments));
new(Src, RoutingKey, Dst, Arguments) ->
    #binding{source = Src, key = RoutingKey, destination = Dst, args = Arguments}.


-define(INFO_KEYS, [source_name, source_kind,
                    destination_name, destination_kind,
                    routing_key, arguments,
                    vhost]).

%% Global table recovery

recover() ->
    rabbit_db_binding:recover().

%% Virtual host-specific recovery

-spec recover([rabbit_exchange:name()], [rabbit_amqqueue:name()]) ->
                        'ok'.
recover(XNames, QNames) ->
    XNameSet = sets:from_list(XNames),
    QNameSet = sets:from_list(QNames),
    SelectSet = fun (#resource{kind = exchange}) -> XNameSet;
                    (#resource{kind = queue})    -> QNameSet
                end,
    {ok, Gatherer} = gatherer:start_link(),
    rabbit_db_binding:recover(
      fun(Binding, Src, Dst, Fun) ->
              recover_semi_durable_route(Gatherer, Binding, Src, Dst, SelectSet(Dst), Fun)
      end),
    empty = gatherer:out(Gatherer),
    ok = gatherer:stop(Gatherer),
    ok.

recover_semi_durable_route(Gatherer, Binding, Src, Dst, ToRecover, Fun) ->
    case sets:is_element(Dst, ToRecover) of
        true  -> {ok, X} = rabbit_exchange:lookup(Src),
                 ok = gatherer:fork(Gatherer),
                 ok = worker_pool:submit_async(
                        fun () ->
                                Fun(Binding, X),
                                gatherer:finish(Gatherer)
                        end);
        false -> ok
    end.

-spec exists(rabbit_types:binding()) -> boolean() | bind_errors().

exists(#binding{source = ?DEFAULT_EXCHANGE(_),
                destination = #resource{kind = queue, name = QName} = Queue,
                key = QName,
                args = []}) ->
    rabbit_amqqueue:exists(Queue);
exists(Binding0) ->
    Binding = sort_args(Binding0),
    rabbit_db_binding:exists(Binding).

-spec add(rabbit_types:binding(), rabbit_types:username()) -> bind_res().

add(Binding, ActingUser) -> add(Binding, fun (_Src, _Dst) -> ok end, ActingUser).

-spec add(rabbit_types:binding(), inner_fun(), rabbit_types:username()) -> bind_res().

add(Binding0, InnerFun, ActingUser) ->
    Binding = sort_args(Binding0),
    case
        rabbit_db_binding:create(Binding, binding_checks(Binding, InnerFun))
    of
        ok ->
            ok = rabbit_event:notify(
                   binding_created,
                   info(Binding) ++ [{user_who_performed_action, ActingUser}]);
        Err ->
            Err
    end.

binding_type(Src, Dst) ->
    binding_type0(durable(Src), durable(Dst)).

binding_type0(true, true) ->
    durable;
binding_type0(false, true) ->
    semi_durable;
binding_type0(_, _) ->
    transient.

-spec remove(rabbit_types:binding(), rabbit_types:username()) -> bind_res().
remove(Binding, ActingUser) -> remove(Binding, fun (_Src, _Dst) -> ok end, ActingUser).

-spec remove(rabbit_types:binding(), inner_fun(), rabbit_types:username()) -> bind_res().
remove(Binding0, InnerFun, ActingUser) ->
    Binding = sort_args(Binding0),
    case
        rabbit_db_binding:delete(Binding, InnerFun)
    of
        {error, _} = Err ->
            Err;
        ok ->
            ok;
        {ok, Deletions} ->
            notify_deletions(Deletions, ActingUser)
    end.

-spec list_explicit() -> bindings().

list_explicit() ->
    rabbit_db_binding:get_all().

-spec list(rabbit_types:vhost()) -> bindings().

list(VHostPath) ->
    ExplicitBindings = rabbit_db_binding:get_all(VHostPath),
    implicit_bindings(VHostPath) ++ ExplicitBindings.

-spec list_for_source
        (rabbit_types:binding_source()) -> bindings().

list_for_source(?DEFAULT_EXCHANGE(VHostPath)) ->
    implicit_bindings(VHostPath);

list_for_source(Resource) ->
    rabbit_db_binding:get_all_for_source(Resource).

-spec list_for_destination
        (rabbit_types:binding_destination()) -> bindings().

list_for_destination(DstName = #resource{}) ->
    ExplicitBindings = rabbit_db_binding:get_all_for_destination(DstName),
    implicit_for_destination(DstName) ++ ExplicitBindings.

implicit_bindings(VHostPath) ->
    DstQueues = rabbit_amqqueue:list_names(VHostPath),
    [ #binding{source = ?DEFAULT_EXCHANGE(VHostPath),
               destination = DstQueue,
               key = QName,
               args = []}
      || DstQueue = #resource{name = QName} <- DstQueues ].

implicit_for_destination(DstQueue = #resource{kind = queue,
                                              virtual_host = VHostPath,
                                              name = QName}) ->
    [#binding{source = ?DEFAULT_EXCHANGE(VHostPath),
              destination = DstQueue,
              key = QName,
              args = []}];
implicit_for_destination(_) ->
    [].

-spec list_for_source_and_destination(rabbit_types:binding_source(), rabbit_types:binding_destination()) ->
    bindings().
list_for_source_and_destination(SrcName, DstName) ->
    list_for_source_and_destination(SrcName, DstName, false).

-spec list_for_source_and_destination(rabbit_types:binding_source(), rabbit_types:binding_destination(), boolean()) ->
    bindings().
list_for_source_and_destination(?DEFAULT_EXCHANGE(VHostPath),
                                #resource{kind = queue,
                                          virtual_host = VHostPath,
                                          name = QName} = DstQueue,
                                _Reverse) ->
    [#binding{source = ?DEFAULT_EXCHANGE(VHostPath),
              destination = DstQueue,
              key = QName,
              args = []}];
list_for_source_and_destination(SrcName, DstName, Reverse) ->
    rabbit_db_binding:get_all(SrcName, DstName, Reverse).

-spec info_keys() -> rabbit_types:info_keys().

info_keys() -> ?INFO_KEYS.

map(VHostPath, F) ->
    %% TODO: there is scope for optimisation here, e.g. using a
    %% cursor, parallelising the function invocation
    lists:map(F, list(VHostPath)).

infos(Items, B) -> [{Item, i(Item, B)} || Item <- Items].

i(source_name,      #binding{source      = SrcName})    -> SrcName#resource.name;
i(source_kind,      #binding{source      = SrcName})    -> SrcName#resource.kind;
i(vhost,            #binding{source      = SrcName})    -> SrcName#resource.virtual_host;
i(destination_name, #binding{destination = DstName})    -> DstName#resource.name;
i(destination_kind, #binding{destination = DstName})    -> DstName#resource.kind;
i(routing_key,      #binding{key         = RoutingKey}) -> RoutingKey;
i(arguments,        #binding{args        = Arguments})  -> Arguments;
i(Item, _) -> throw({bad_argument, Item}).

-spec info(rabbit_types:binding()) -> rabbit_types:infos().

info(B = #binding{}) -> infos(?INFO_KEYS, B).

-spec info(rabbit_types:binding(), rabbit_types:info_keys()) ->
          rabbit_types:infos().

info(B = #binding{}, Items) -> infos(Items, B).

-spec info_all(rabbit_types:vhost()) -> [rabbit_types:infos()].

info_all(VHostPath) -> map(VHostPath, fun (B) -> info(B) end).

-spec info_all(rabbit_types:vhost(), rabbit_types:info_keys()) ->
          [rabbit_types:infos()].

info_all(VHostPath, Items) -> map(VHostPath, fun (B) -> info(B, Items) end).

-spec info_all(rabbit_types:vhost(), rabbit_types:info_keys(),
                    reference(), pid()) -> 'ok'.

info_all(VHostPath, Items, Ref, AggregatorPid) ->
    rabbit_control_misc:emitting_map(
      AggregatorPid, Ref, fun(B) -> info(B, Items) end, list(VHostPath)).

%%----------------------------------------------------------------------------

durable(#exchange{durable = D}) -> D;
durable(Q) when ?is_amqqueue(Q) ->
    amqqueue:is_durable(Q).

sort_args(#binding{args = Arguments} = Binding) ->
    SortedArgs = rabbit_misc:sort_field_table(Arguments),
    Binding#binding{args = SortedArgs}.

%% Requires that its input binding list is sorted in exchange-name
%% order, so that the grouping of bindings (for passing to
%% group_bindings_and_auto_delete1) works properly.
group_bindings_fold(Fun, Bindings, OnlyDurable) ->
    group_bindings_fold(Fun, new_deletions(), Bindings, OnlyDurable).

group_bindings_fold(_Fun, Acc, [], _OnlyDurable) ->
    Acc;
group_bindings_fold(Fun, Acc, [B = #binding{source = SrcName} | Bs],
                    OnlyDurable) ->
    group_bindings_fold(Fun, SrcName, Acc, Bs, [B], OnlyDurable).

-spec group_bindings_fold(Fun, Name, Deletions, [Binding], [Binding], OnlyDurable)
                         -> Ret when
      Fun :: fun((Name, [Binding], Deletions, OnlyDurable) ->
                        Deletions),
      Name :: rabbit_exchange:name(),
      Deletions :: rabbit_binding:deletions(),
      Binding :: rabbit_types:binding(),
      OnlyDurable :: boolean(),
      Ret :: Deletions.
group_bindings_fold(
  Fun, SrcName, Acc, [B = #binding{source = SrcName} | Bs], Bindings,
  OnlyDurable) ->
    group_bindings_fold(Fun, SrcName, Acc, Bs, [B | Bindings], OnlyDurable);
group_bindings_fold(Fun, SrcName, Acc, Removed, Bindings, OnlyDurable) ->
    %% Either Removed is [], or its head has a non-matching SrcName.
    group_bindings_fold(Fun, Fun(SrcName, Bindings, Acc, OnlyDurable), Removed,
                        OnlyDurable).

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

index_route(#route{binding = #binding{source = Source,
                                      key = Key,
                                      destination = Destination,
                                      args = Args}}) ->
    #index_route{source_key = {Source, Key},
                 destination = Destination,
                 args = Args}.

%% ----------------------------------------------------------------------------
%% Binding / exchange deletion abstraction API
%% ----------------------------------------------------------------------------

anything_but( NotThis, NotThis, NotThis) -> NotThis;
anything_but( NotThis, NotThis,    This) -> This;
anything_but( NotThis,    This, NotThis) -> This;
anything_but(_NotThis,    This,    This) -> This.

-spec new_deletions() -> deletions().

new_deletions() -> dict:new().

-spec add_deletion
        (rabbit_exchange:name(),
         {'undefined' | rabbit_types:exchange(),
          'deleted' | 'not_deleted',
          bindings()},
         deletions()) ->
            deletions().

add_deletion(XName, Entry, Deletions) ->
    dict:update(XName, fun (Entry1) -> merge_entry(Entry1, Entry) end,
                Entry, Deletions).

-spec combine_deletions(deletions(), deletions()) -> deletions().

combine_deletions(Deletions1, Deletions2) ->
    dict:merge(fun (_XName, Entry1, Entry2) -> merge_entry(Entry1, Entry2) end,
               Deletions1, Deletions2).

merge_entry({X1, Deleted1, Bindings1}, {X2, Deleted2, Bindings2}) ->
    {anything_but(undefined, X1, X2),
     anything_but(not_deleted, Deleted1, Deleted2),
     Bindings1 ++ Bindings2};
merge_entry({X1, Deleted1, Bindings1, none}, {X2, Deleted2, Bindings2, none}) ->
    {anything_but(undefined, X1, X2),
     anything_but(not_deleted, Deleted1, Deleted2),
     Bindings1 ++ Bindings2, none}.

notify_deletions({error, not_found}, _) ->
    ok;
notify_deletions(Deletions, ActingUser) ->
    dict:fold(fun (XName, {_X, deleted, Bs, _}, ok) ->
                      notify_exchange_deletion(XName, ActingUser),
                      notify_bindings_deletion(Bs, ActingUser);
                  (_XName, {_X, not_deleted, Bs, _}, ok) ->
                      notify_bindings_deletion(Bs, ActingUser);
                  (XName, {_X, deleted, Bs}, ok) ->
                      notify_exchange_deletion(XName, ActingUser),
                      notify_bindings_deletion(Bs, ActingUser);
                  (_XName, {_X, not_deleted, Bs}, ok) ->
                      notify_bindings_deletion(Bs, ActingUser)
              end, ok, Deletions).

notify_exchange_deletion(XName, ActingUser) ->
    ok = rabbit_event:notify(
           exchange_deleted,
           [{name, XName},
            {user_who_performed_action, ActingUser}]).

notify_bindings_deletion(Bs, ActingUser) ->
    [rabbit_event:notify(binding_deleted,
                         info(B) ++ [{user_who_performed_action, ActingUser}])
     || B <- Bs],
    ok.

-spec process_deletions(deletions()) -> deletions().
process_deletions(Deletions) ->
    dict:map(fun (_XName, {X, deleted, Bindings}) ->
                     Bs = lists:flatten(Bindings),
                     Serial = rabbit_exchange:serial(X),
                     rabbit_exchange:callback(X, delete, Serial, [X]),
                     {X, deleted, Bs, none};
                 (_XName, {X, not_deleted, Bindings}) ->
                     Bs = lists:flatten(Bindings),
                     Serial = rabbit_exchange:serial(X),
                     rabbit_exchange:callback(X, remove_bindings, Serial, [X, Bs]),
                     {X, not_deleted, Bs, none}
             end, Deletions).

binding_checks(Binding, InnerFun) ->
    fun(Src, Dst) ->
            case rabbit_exchange:validate_binding(Src, Binding) of
                ok ->
                    %% this argument is used to check queue exclusivity;
                    %% in general, we want to fail on that in preference to
                    %% anything else
                    InnerFun(Src, Dst);
                Err ->
                    Err
            end
    end.
