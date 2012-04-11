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

-module(rabbit_exchange).
-include("rabbit.hrl").
-include("rabbit_framing.hrl").

-export([recover/0, callback/3, declare/6,
         assert_equivalence/6, assert_args_equivalence/2, check_type/1,
         lookup/1, lookup_or_die/1, list/1, update_scratch/2,
         info_keys/0, info/1, info/2, info_all/1, info_all/2,
         route/2, delete/2]).
%% these must be run inside a mnesia tx
-export([maybe_auto_delete/1, serial/1, peek_serial/1]).

%%----------------------------------------------------------------------------

-ifdef(use_specs).

-export_type([name/0, type/0]).

-type(name() :: rabbit_types:r('exchange')).
-type(type() :: atom()).
-type(fun_name() :: atom()).

-spec(recover/0 :: () -> [name()]).
-spec(callback/3:: (rabbit_types:exchange(), fun_name(), [any()]) -> 'ok').
-spec(declare/6 ::
        (name(), type(), boolean(), boolean(), boolean(),
         rabbit_framing:amqp_table())
        -> rabbit_types:exchange()).
-spec(check_type/1 ::
        (binary()) -> atom() | rabbit_types:connection_exit()).
-spec(assert_equivalence/6 ::
        (rabbit_types:exchange(), atom(), boolean(), boolean(), boolean(),
         rabbit_framing:amqp_table())
        -> 'ok' | rabbit_types:connection_exit()).
-spec(assert_args_equivalence/2 ::
        (rabbit_types:exchange(), rabbit_framing:amqp_table())
        -> 'ok' | rabbit_types:connection_exit()).
-spec(lookup/1 ::
        (name()) -> rabbit_types:ok(rabbit_types:exchange()) |
                    rabbit_types:error('not_found')).
-spec(lookup_or_die/1 ::
        (name()) -> rabbit_types:exchange() |
                    rabbit_types:channel_exit()).
-spec(list/1 :: (rabbit_types:vhost()) -> [rabbit_types:exchange()]).
-spec(update_scratch/2 :: (name(), fun((any()) -> any())) -> 'ok').
-spec(info_keys/0 :: () -> rabbit_types:info_keys()).
-spec(info/1 :: (rabbit_types:exchange()) -> rabbit_types:infos()).
-spec(info/2 ::
        (rabbit_types:exchange(), rabbit_types:info_keys())
        -> rabbit_types:infos()).
-spec(info_all/1 :: (rabbit_types:vhost()) -> [rabbit_types:infos()]).
-spec(info_all/2 ::(rabbit_types:vhost(), rabbit_types:info_keys())
                   -> [rabbit_types:infos()]).
-spec(route/2 :: (rabbit_types:exchange(), rabbit_types:delivery())
                 -> [rabbit_amqqueue:name()]).
-spec(delete/2 ::
        (name(), boolean())-> 'ok' |
                              rabbit_types:error('not_found') |
                              rabbit_types:error('in_use')).
-spec(maybe_auto_delete/1::
        (rabbit_types:exchange())
        -> 'not_deleted' | {'deleted', rabbit_binding:deletions()}).
-spec(serial/1 :: (rabbit_types:exchange()) -> 'none' | pos_integer()).
-spec(peek_serial/1 :: (name()) -> pos_integer() | 'undefined').

-endif.

%%----------------------------------------------------------------------------

-define(INFO_KEYS, [name, type, durable, auto_delete, internal, arguments]).

recover() ->
    Xs = rabbit_misc:table_filter(
           fun (#exchange{name = XName}) ->
                   mnesia:read({rabbit_exchange, XName}) =:= []
           end,
           fun (X, Tx) ->
                   case Tx of
                       true  -> store(X);
                       false -> ok
                   end,
                   rabbit_exchange:callback(X, create, [map_create_tx(Tx), X])
           end,
           rabbit_durable_exchange),
    [XName || #exchange{name = XName} <- Xs].

callback(#exchange{type = XType}, Fun, Args) ->
    apply(type_to_module(XType), Fun, Args).

declare(XName, Type, Durable, AutoDelete, Internal, Args) ->
    X = #exchange{name        = XName,
                  type        = Type,
                  durable     = Durable,
                  auto_delete = AutoDelete,
                  internal    = Internal,
                  arguments   = Args},
    XT = type_to_module(Type),
    %% We want to upset things if it isn't ok
    ok = XT:validate(X),
    rabbit_misc:execute_mnesia_transaction(
      fun () ->
              case mnesia:wread({rabbit_exchange, XName}) of
                  [] ->
                      store(X),
                      ok = case Durable of
                               true  -> mnesia:write(rabbit_durable_exchange,
                                                     X, write);
                               false -> ok
                           end,
                      {new, X};
                  [ExistingX] ->
                      {existing, ExistingX}
              end
      end,
      fun ({new, Exchange}, Tx) ->
              ok = XT:create(map_create_tx(Tx), Exchange),
              rabbit_event:notify_if(not Tx, exchange_created, info(Exchange)),
              Exchange;
          ({existing, Exchange}, _Tx) ->
              Exchange;
          (Err, _Tx) ->
              Err
      end).

map_create_tx(true)  -> transaction;
map_create_tx(false) -> none.

store(X = #exchange{name = Name, type = Type}) ->
    ok = mnesia:write(rabbit_exchange, X, write),
    case (type_to_module(Type)):serialise_events() of
        true  -> S = #exchange_serial{name = Name, next = 1},
                 ok = mnesia:write(rabbit_exchange_serial, S, write);
        false -> ok
    end.

%% Used with binaries sent over the wire; the type may not exist.
check_type(TypeBin) ->
    case rabbit_registry:binary_to_type(TypeBin) of
        {error, not_found} ->
            rabbit_misc:protocol_error(
              command_invalid, "unknown exchange type '~s'", [TypeBin]);
        T ->
            case rabbit_registry:lookup_module(exchange, T) of
                {error, not_found} -> rabbit_misc:protocol_error(
                                        command_invalid,
                                        "invalid exchange type '~s'", [T]);
                {ok, _Module}      -> T
            end
    end.

assert_equivalence(X = #exchange{ durable     = Durable,
                                  auto_delete = AutoDelete,
                                  internal    = Internal,
                                  type        = Type},
                   Type, Durable, AutoDelete, Internal, RequiredArgs) ->
    (type_to_module(Type)):assert_args_equivalence(X, RequiredArgs);
assert_equivalence(#exchange{ name = Name },
                   _Type, _Durable, _Internal, _AutoDelete, _Args) ->
    rabbit_misc:protocol_error(
      precondition_failed,
      "cannot redeclare ~s with different type, durable, "
      "internal or autodelete value",
      [rabbit_misc:rs(Name)]).

assert_args_equivalence(#exchange{ name = Name, arguments = Args },
                        RequiredArgs) ->
    %% The spec says "Arguments are compared for semantic
    %% equivalence".  The only arg we care about is
    %% "alternate-exchange".
    rabbit_misc:assert_args_equivalence(Args, RequiredArgs, Name,
                                        [<<"alternate-exchange">>]).

lookup(Name) ->
    rabbit_misc:dirty_read({rabbit_exchange, Name}).

lookup_or_die(Name) ->
    case lookup(Name) of
        {ok, X}            -> X;
        {error, not_found} -> rabbit_misc:not_found(Name)
    end.

list(VHostPath) ->
    mnesia:dirty_match_object(
      rabbit_exchange,
      #exchange{name = rabbit_misc:r(VHostPath, exchange), _ = '_'}).

update_scratch(Name, Fun) ->
    rabbit_misc:execute_mnesia_transaction(
      fun() ->
              case mnesia:wread({rabbit_exchange, Name}) of
                  [X = #exchange{durable = Durable, scratch = Scratch}] ->
                      X1 = X#exchange{scratch = Fun(Scratch)},
                      ok = mnesia:write(rabbit_exchange, X1, write),
                      case Durable of
                          true -> ok = mnesia:write(rabbit_durable_exchange,
                                                    X1, write);
                          _    -> ok
                      end;
                  [] ->
                      ok
              end
      end).

info_keys() -> ?INFO_KEYS.

map(VHostPath, F) ->
    %% TODO: there is scope for optimisation here, e.g. using a
    %% cursor, parallelising the function invocation
    lists:map(F, list(VHostPath)).

infos(Items, X) -> [{Item, i(Item, X)} || Item <- Items].

i(name,        #exchange{name        = Name})       -> Name;
i(type,        #exchange{type        = Type})       -> Type;
i(durable,     #exchange{durable     = Durable})    -> Durable;
i(auto_delete, #exchange{auto_delete = AutoDelete}) -> AutoDelete;
i(internal,    #exchange{internal    = Internal})   -> Internal;
i(arguments,   #exchange{arguments   = Arguments})  -> Arguments;
i(Item, _) -> throw({bad_argument, Item}).

info(X = #exchange{}) -> infos(?INFO_KEYS, X).

info(X = #exchange{}, Items) -> infos(Items, X).

info_all(VHostPath) -> map(VHostPath, fun (X) -> info(X) end).

info_all(VHostPath, Items) -> map(VHostPath, fun (X) -> info(X, Items) end).

%% Optimisation
route(#exchange{name = #resource{name = <<"">>, virtual_host = VHost}},
      #delivery{message = #basic_message{routing_keys = RKs}}) ->
    [rabbit_misc:r(VHost, queue, RK) || RK <- lists:usort(RKs)];

route(X = #exchange{name = XName}, Delivery) ->
    route1(Delivery, {queue:from_list([X]), XName, []}).

route1(Delivery, {WorkList, SeenXs, QNames}) ->
    case queue:out(WorkList) of
        {empty, _WorkList} ->
            lists:usort(QNames);
        {{value, X = #exchange{type = Type}}, WorkList1} ->
            DstNames = process_alternate(
                         X, ((type_to_module(Type)):route(X, Delivery))),
            route1(Delivery,
                   lists:foldl(fun process_route/2, {WorkList1, SeenXs, QNames},
                               DstNames))
    end.

process_alternate(#exchange{arguments = []}, Results) -> %% optimisation
     Results;
process_alternate(#exchange{name = XName, arguments = Args}, []) ->
    case rabbit_misc:r_arg(XName, exchange, Args, <<"alternate-exchange">>) of
        undefined -> [];
        AName     -> [AName]
    end;
process_alternate(_X, Results) ->
    Results.

process_route(#resource{kind = exchange} = XName,
              {_WorkList, XName, _QNames} = Acc) ->
    Acc;
process_route(#resource{kind = exchange} = XName,
              {WorkList, #resource{kind = exchange} = SeenX, QNames}) ->
    {case lookup(XName) of
         {ok, X}            -> queue:in(X, WorkList);
         {error, not_found} -> WorkList
     end, gb_sets:from_list([SeenX, XName]), QNames};
process_route(#resource{kind = exchange} = XName,
              {WorkList, SeenXs, QNames} = Acc) ->
    case gb_sets:is_element(XName, SeenXs) of
        true  -> Acc;
        false -> {case lookup(XName) of
                      {ok, X}            -> queue:in(X, WorkList);
                      {error, not_found} -> WorkList
                  end, gb_sets:add_element(XName, SeenXs), QNames}
    end;
process_route(#resource{kind = queue} = QName,
              {WorkList, SeenXs, QNames}) ->
    {WorkList, SeenXs, [QName | QNames]}.

call_with_exchange(XName, Fun) ->
    rabbit_misc:execute_mnesia_tx_with_tail(
      fun () -> case mnesia:read({rabbit_exchange, XName}) of
                    []  -> rabbit_misc:const({error, not_found});
                    [X] -> Fun(X)
                end
      end).

delete(XName, IfUnused) ->
    Fun = case IfUnused of
              true  -> fun conditional_delete/1;
              false -> fun unconditional_delete/1
          end,
    call_with_exchange(
      XName,
      fun (X) ->
              case Fun(X) of
                  {deleted, X, Bs, Deletions} ->
                      rabbit_binding:process_deletions(
                        rabbit_binding:add_deletion(
                          XName, {X, deleted, Bs}, Deletions));
                  {error, _InUseOrNotFound} = E ->
                      rabbit_misc:const(E)
              end
      end).

maybe_auto_delete(#exchange{auto_delete = false}) ->
    not_deleted;
maybe_auto_delete(#exchange{auto_delete = true} = X) ->
    case conditional_delete(X) of
        {error, in_use}             -> not_deleted;
        {deleted, X, [], Deletions} -> {deleted, Deletions}
    end.

conditional_delete(X = #exchange{name = XName}) ->
    case rabbit_binding:has_for_source(XName) of
        false  -> unconditional_delete(X);
        true   -> {error, in_use}
    end.

unconditional_delete(X = #exchange{name = XName}) ->
    ok = mnesia:delete({rabbit_durable_exchange, XName}),
    ok = mnesia:delete({rabbit_exchange, XName}),
    ok = mnesia:delete({rabbit_exchange_serial, XName}),
    Bindings = rabbit_binding:remove_for_source(XName),
    {deleted, X, Bindings, rabbit_binding:remove_for_destination(XName)}.

serial(#exchange{name = XName, type = Type}) ->
    case (type_to_module(Type)):serialise_events() of
        true  -> next_serial(XName);
        false -> none
    end.

next_serial(XName) ->
    [#exchange_serial{next = Serial}] =
        mnesia:read(rabbit_exchange_serial, XName, write),
    ok = mnesia:write(rabbit_exchange_serial,
                      #exchange_serial{name = XName, next = Serial + 1}, write),
    Serial.

peek_serial(XName) ->
    case mnesia:read({rabbit_exchange_serial, XName}) of
        [#exchange_serial{next = Serial}]  -> Serial;
        _                                  -> undefined
    end.

invalid_module(T) ->
    rabbit_log:warning(
      "Could not find exchange type ~s.~n", [T]),
    put({xtype_to_module, T}, rabbit_exchange_type_invalid),
    rabbit_exchange_type_invalid.

%% Used with atoms from records; e.g., the type is expected to exist.
type_to_module(T) ->
    case get({xtype_to_module, T}) of
        undefined ->
            case rabbit_registry:lookup_module(exchange, T) of
                {ok, Module}       -> put({xtype_to_module, T}, Module),
                                      Module;
                {error, not_found} -> invalid_module(T)
            end;
        Module ->
            Module
    end.
