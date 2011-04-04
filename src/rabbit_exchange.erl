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

-module(rabbit_exchange).
-include("rabbit.hrl").
-include("rabbit_framing.hrl").

-export([recover/0, callback/3, declare/6,
         assert_equivalence/6, assert_args_equivalence/2, check_type/1,
         lookup/1, lookup_or_die/1, list/1,
         info_keys/0, info/1, info/2, info_all/1, info_all/2,
         publish/2, delete/2]).
%% this must be run inside a mnesia tx
-export([maybe_auto_delete/1]).

%%----------------------------------------------------------------------------

-ifdef(use_specs).

-export_type([name/0, type/0]).

-type(name() :: rabbit_types:r('exchange')).
-type(type() :: atom()).
-type(fun_name() :: atom()).

-spec(recover/0 :: () -> [rabbit_exchange:name()]).
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
-spec(info_keys/0 :: () -> rabbit_types:info_keys()).
-spec(info/1 :: (rabbit_types:exchange()) -> rabbit_types:infos()).
-spec(info/2 ::
        (rabbit_types:exchange(), rabbit_types:info_keys())
        -> rabbit_types:infos()).
-spec(info_all/1 :: (rabbit_types:vhost()) -> [rabbit_types:infos()]).
-spec(info_all/2 ::(rabbit_types:vhost(), rabbit_types:info_keys())
                   -> [rabbit_types:infos()]).
-spec(publish/2 :: (rabbit_types:exchange(), rabbit_types:delivery())
                   -> {rabbit_router:routing_result(), [pid()]}).
-spec(delete/2 ::
        (name(), boolean())-> 'ok' |
                              rabbit_types:error('not_found') |
                              rabbit_types:error('in_use')).
-spec(maybe_auto_delete/1::
        (rabbit_types:exchange())
        -> 'not_deleted' | {'deleted', rabbit_binding:deletions()}).

-endif.

%%----------------------------------------------------------------------------

-define(INFO_KEYS, [name, type, durable, auto_delete, internal, arguments]).

recover() ->
    Xs = rabbit_misc:table_fold(
           fun (X = #exchange{name = XName}, Acc) ->
                   case mnesia:read({rabbit_exchange, XName}) of
                       []  -> ok = mnesia:write(rabbit_exchange, X, write),
                              [X | Acc];
                       [_] -> Acc
                   end
           end, [], rabbit_durable_exchange),
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
    %% We want to upset things if it isn't ok
    ok = (type_to_module(Type)):validate(X),
    rabbit_misc:execute_mnesia_transaction(
      fun () ->
              case mnesia:wread({rabbit_exchange, XName}) of
                  [] ->
                      ok = mnesia:write(rabbit_exchange, X, write),
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
              ok = (type_to_module(Type)):create(Tx, Exchange),
              rabbit_event:notify_if(not Tx, exchange_created, info(Exchange)),
              Exchange;
          ({existing, Exchange}, _Tx) ->
              Exchange;
          (Err, _Tx) ->
              Err
      end).

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

publish(X = #exchange{name = XName}, Delivery) ->
    rabbit_router:deliver(
      route(Delivery, {queue:from_list([X]), XName, []}),
      Delivery).

route(Delivery, {WorkList, SeenXs, QNames}) ->
    case queue:out(WorkList) of
        {empty, _WorkList} ->
            lists:usort(QNames);
        {{value, X = #exchange{type = Type}}, WorkList1} ->
            DstNames = process_alternate(
                         X, ((type_to_module(Type)):route(X, Delivery))),
            route(Delivery,
                  lists:foldl(fun process_route/2, {WorkList1, SeenXs, QNames},
                              DstNames))
    end.

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

call_with_exchange(XName, Fun, PrePostCommitFun) ->
    rabbit_misc:execute_mnesia_transaction(
      fun () -> case mnesia:read({rabbit_exchange, XName}) of
                    []  -> {error, not_found};
                    [X] -> Fun(X)
                end
      end, PrePostCommitFun).

delete(XName, IfUnused) ->
    call_with_exchange(
      XName,
      case IfUnused of
          true  -> fun conditional_delete/1;
          false -> fun unconditional_delete/1
      end,
      fun ({deleted, X, Bs, Deletions}, Tx) ->
              ok = rabbit_binding:process_deletions(
                     rabbit_binding:add_deletion(
                       XName, {X, deleted, Bs}, Deletions), Tx);
          (Error = {error, _InUseOrNotFound}, _Tx) ->
              Error
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
    Bindings = rabbit_binding:remove_for_source(XName),
    {deleted, X, Bindings, rabbit_binding:remove_for_destination(XName)}.

%% Used with atoms from records; e.g., the type is expected to exist.
type_to_module(T) ->
    {ok, Module} = rabbit_registry:lookup_module(exchange, T),
    Module.
