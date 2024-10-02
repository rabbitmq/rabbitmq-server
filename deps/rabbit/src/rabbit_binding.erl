%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2024 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_binding).
-include_lib("rabbit_common/include/rabbit.hrl").
-include("amqqueue.hrl").

-export([recover/0, recover/2, exists/1, add/2, add/3, remove/2, remove/3]).
-export([list/1, list_for_source/1, list_for_destination/1,
         list_for_source_and_destination/2, list_for_source_and_destination/3,
         list_explicit/0]).
-export([new_deletions/0, combine_deletions/2, add_deletion/5,
         process_deletions/1, notify_deletions/2, group_bindings_fold/3]).
-export([info_keys/0, info/1, info/2, info_all/1, info_all/2, info_all/4]).

-export([reverse_binding/1]).
-export([new/4]).
-export([reverse_route/1, index_route/1]).
-export([binding_type/2]).

%% For testing only
-export([fetch_deletion/2]).

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
                            rabbit_types:error(rabbit_types:amqp_error()) |
                            rabbit_khepri:timeout_error().
-type bind_res() :: bind_ok_or_error() | rabbit_misc:thunk(bind_ok_or_error()).
-type inner_fun() ::
        fun((rabbit_types:exchange(),
             rabbit_types:exchange() | amqqueue:amqqueue()) ->
                   rabbit_types:ok_or_error(rabbit_types:amqp_error())).
-type bindings() :: [rabbit_types:binding()].

-record(deletion, {exchange :: rabbit_types:exchange(),
                   %% Whether the exchange was deleted.
                   deleted :: boolean(),
                   bindings :: sets:set(rabbit_types:binding())}).

-opaque deletions() :: #{XName :: rabbit_exchange:name() => #deletion{}}.

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
        true  ->
            case rabbit_exchange:lookup(Src) of
                {ok, X} ->
                    ok = gatherer:fork(Gatherer),
                    ok = worker_pool:submit_async(
                           fun () ->
                                   Fun(Binding, X),
                                   gatherer:finish(Gatherer)
                           end);
                {error, not_found}=Error ->
                    rabbit_log:warning(
                      "expected exchange ~tp to exist during recovery, "
                      "error: ~tp", [Src, Error]),
                    ok
            end;
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
%%
%% `deletions()' describe a set of removals of bindings and/or exchanges from
%% the metadata store.
%%
%% This deletion collection is used for two purposes:
%%
%% <ul>
%% <li>"<em>Processing</em>" of deletions. Processing here means that the
%% exchanges and bindings are passed into the {@link rabbit_exchange}
%% callbacks. When an exchange is deleted the `rabbit_exchange:delete/1'
%% callback is invoked and when the exchange is not deleted but some bindings
%% are deleted the `rabbit_exchange:remove_bindings/2' is invoked.</li>
%% <li><em>Notification</em> of metadata deletion. Like other internal
%% notifications, {@link rabbit_binding:notify_deletions()} uses {@link
%% rabbit_event} to notify any interested consumers of a resource deletion.
%% An example consumer of {@link rabbit_event} is the `rabbitmq_event_exchange'
%% plugin which publishes these notifications as messages.</li>
%% </ul>
%%
%% The point of collecting deletions into this opaque type is to be able to
%% collect all bindings deleted for a given exchange into a list. This allows
%% us to invoke the `rabbit_exchange:remove_bindings/2' callback with all
%% deleted bindings at once rather than passing each deleted binding
%% individually.

-spec new_deletions() -> deletions().

new_deletions() -> #{}.

-spec add_deletion(XName, X, XDeleted, Bindings, Deletions) -> Deletions1
    when
      XName :: rabbit_exchange:name(),
      X :: rabbit_types:exchange(),
      XDeleted :: deleted | not_deleted,
      Bindings :: bindings(),
      Deletions :: deletions(),
      Deletions1 :: deletions().

add_deletion(XName, X, WasDeleted, Bindings, Deletions)
  when (WasDeleted =:= deleted orelse WasDeleted =:= not_deleted) andalso
       is_list(Bindings) andalso is_map(Deletions) ->
    WasDeleted1 = case WasDeleted of
                      deleted -> true;
                      not_deleted -> false
                  end,
    Bindings1 = sets:from_list(Bindings, [{version, 2}]),
    Deletion = #deletion{exchange = X,
                         deleted = WasDeleted1,
                         bindings = Bindings1},
    maps:update_with(
      XName,
      fun(Deletion1) ->
              merge_deletion(Deletion1, Deletion)
      end, Deletion, Deletions).

-spec combine_deletions(deletions(), deletions()) -> deletions().

combine_deletions(Deletions1, Deletions2)
  when is_map(Deletions1) andalso is_map(Deletions2) ->
    maps:merge_with(
      fun (_XName, Deletion1, Deletion2) ->
              merge_deletion(Deletion1, Deletion2)
      end, Deletions1, Deletions2).

merge_deletion(
  #deletion{deleted = Deleted1, bindings = Bindings1},
  #deletion{exchange = X2, deleted = Deleted2, bindings = Bindings2}) ->
    %% Assume that X2 is more up to date than X1.
    X = X2,
    Deleted = Deleted1 orelse Deleted2,
    Bindings = sets:union(Bindings1, Bindings2),
    #deletion{exchange = X,
              deleted = Deleted,
              bindings = Bindings}.

-spec notify_deletions(Deletions, ActingUser) -> ok when
      Deletions :: rabbit_binding:deletions(),
      ActingUser :: rabbit_types:username().

notify_deletions(Deletions, ActingUser) when is_map(Deletions) ->
    maps:foreach(
      fun (XName, #deletion{deleted = XDeleted, bindings = Bindings}) ->
              case XDeleted of
                  true ->
                      notify_exchange_deletion(XName, ActingUser),
                      notify_bindings_deletion(Bindings, ActingUser);
                  false ->
                      notify_bindings_deletion(Bindings, ActingUser)
              end
      end, Deletions).

notify_exchange_deletion(XName, ActingUser) ->
    ok = rabbit_event:notify(
           exchange_deleted,
           [{name, XName},
            {user_who_performed_action, ActingUser}]).

notify_bindings_deletion(Bindings, ActingUser) ->
    sets:fold(
      fun(Binding, ok) ->
              rabbit_event:notify(
                binding_deleted,
                info(Binding) ++ [{user_who_performed_action, ActingUser}]),
              ok
      end, ok, Bindings).

-spec process_deletions(deletions()) -> ok.
process_deletions(Deletions) ->
    maps:foreach(
      fun (_XName, #deletion{exchange = X,
                             deleted = XDeleted,
                             bindings = Bindings}) ->
              Serial = rabbit_exchange:serial(X),
              case XDeleted of
                  true ->
                      rabbit_exchange:callback(X, delete, Serial, [X]);
                  false ->
                      Bindings1 = sets:to_list(Bindings),
                      rabbit_exchange:callback(
                        X, remove_bindings, Serial, [X, Bindings1])
              end
      end, Deletions).

-spec fetch_deletion(XName, Deletions) -> Ret when
      XName :: rabbit_exchange:name(),
      Deletions :: deletions(),
      Ret :: {X, WasDeleted, Bindings},
      X :: rabbit_types:exchange(),
      WasDeleted :: deleted | not_deleted,
      Bindings :: bindings().
%% @doc Fetches the deletions for the given exchange name.
%%
%% This function is only intended for use in tests.
%%
%% @private

fetch_deletion(XName, Deletions) ->
    case maps:find(XName, Deletions) of
        {ok, #deletion{exchange = X,
                       deleted = Deleted,
                       bindings = Bindings}} ->
            WasDeleted = case Deleted of
                             true ->
                                 deleted;
                             false ->
                                 not_deleted
                         end,
            Bindings1 = sets:to_list(Bindings),
            {X, WasDeleted, Bindings1};
        error ->
            error
    end.
