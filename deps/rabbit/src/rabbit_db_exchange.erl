%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2023 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_db_exchange).

-include_lib("khepri/include/khepri.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").

-export([
         get_all/0,
         get_all/1,
         get_all_durable/0,
         list/0,
         get/1,
         get_many/1,
         count/0,
         update/2,
         create_or_get/1,
         set/1,
         peek_serial/1,
         next_serial/1,
         delete/2,
         delete_serial/1,
         recover/1,
         match/1,
         exists/1
        ]).

%% Used by other rabbit_db_* modules
-export([
         maybe_auto_delete_in_khepri/2,
         maybe_auto_delete_in_mnesia/2,
         next_serial_in_mnesia_tx/1,
         next_serial_in_khepri_tx/1,
         delete_in_khepri/3,
         delete_in_mnesia/3,
         get_in_khepri_tx/1,
         update_in_mnesia_tx/2,
         update_in_khepri_tx/2,
         path/1
         ]).

%% For testing
-export([clear/0]).

-export([
         khepri_exchange_path/1,
         khepri_exchange_serial_path/1,
         khepri_exchanges_path/0,
         khepri_exchange_serials_path/0
        ]).

-define(MNESIA_TABLE, rabbit_exchange).
-define(MNESIA_DURABLE_TABLE, rabbit_durable_exchange).
-define(MNESIA_SERIAL_TABLE, rabbit_exchange_serial).

%% -------------------------------------------------------------------
%% get_all().
%% -------------------------------------------------------------------

-spec get_all() -> [Exchange] when
      Exchange :: rabbit_types:exchange().
%% @doc Returns all exchange records.
%%
%% @returns the list of exchange records.
%%
%% @private

get_all() ->
    rabbit_khepri:handle_fallback(
      #{mnesia => fun() -> get_all_in_mnesia() end,
        khepri => fun() -> get_all_in_khepri() end
       }).

get_all_in_mnesia() ->
    rabbit_db:list_in_mnesia(?MNESIA_TABLE, #exchange{_ = '_'}).

get_all_in_khepri() ->
    rabbit_db:list_in_khepri(khepri_exchanges_path() ++ [rabbit_khepri:if_has_data_wildcard()]).

-spec get_all(VHostName) -> [Exchange] when
      VHostName :: vhost:name(),
      Exchange :: rabbit_types:exchange().
%% @doc Returns all exchange records in the given virtual host.
%%
%% @returns the list of exchange records.
%%
%% @private

get_all(VHost) ->
    rabbit_khepri:handle_fallback(
      #{mnesia => fun() -> get_all_in_mnesia(VHost) end,
        khepri => fun() -> get_all_in_khepri(VHost) end
       }).

get_all_in_mnesia(VHost) ->
    Match = #exchange{name = rabbit_misc:r(VHost, exchange), _ = '_'},
    rabbit_db:list_in_mnesia(?MNESIA_TABLE, Match).

get_all_in_khepri(VHost) ->
    rabbit_db:list_in_khepri(khepri_exchanges_path() ++ [VHost, rabbit_khepri:if_has_data_wildcard()]).

%% -------------------------------------------------------------------
%% get_all_durable().
%% -------------------------------------------------------------------

-spec get_all_durable() -> [Exchange] when
      Exchange :: rabbit_types:exchange().
%% @doc Returns all durable exchange records.
%%
%% @returns the list of exchange records.
%%
%% @private

get_all_durable() ->
    rabbit_khepri:handle_fallback(
      #{mnesia => fun() -> get_all_durable_in_mnesia() end,
        khepri => fun() -> get_all_durable_in_khepri() end
       }).

get_all_durable_in_mnesia() ->
    rabbit_db:list_in_mnesia(rabbit_durable_exchange, #exchange{_ = '_'}).

get_all_durable_in_khepri() ->
    rabbit_db:list_in_khepri(khepri_exchanges_path() ++ [rabbit_khepri:if_has_data_wildcard()]).

%% -------------------------------------------------------------------
%% list().
%% -------------------------------------------------------------------

-spec list() -> [ExchangeName] when
      ExchangeName :: rabbit_exchange:name().
%% @doc Lists the names of all exchanges.
%%
%% @returns a list of exchange names.
%%
%% @private

list() ->
    rabbit_khepri:handle_fallback(
      #{mnesia => fun() -> list_in_mnesia() end,
        khepri => fun() -> list_in_khepri() end
       }).

list_in_mnesia() ->
    mnesia:dirty_all_keys(?MNESIA_TABLE).

list_in_khepri() ->
    case rabbit_khepri:match(khepri_exchanges_path() ++
                                 [rabbit_khepri:if_has_data_wildcard()]) of
        {ok, Map} ->
            maps:fold(fun(_K, X, Acc) -> [X#exchange.name | Acc] end, [], Map);
        _ ->
            []
    end.

%% -------------------------------------------------------------------
%% get().
%% -------------------------------------------------------------------

-spec get(ExchangeName) -> Ret when
      ExchangeName :: rabbit_exchange:name(),
      Ret :: {ok, Exchange :: rabbit_types:exchange()} | {error, not_found}.
%% @doc Returns the record of the exchange named `Name'.
%%
%% @returns the exchange record or `{error, not_found}' if no exchange is named
%% `Name'.
%%
%% @private

get(Name) ->
    rabbit_khepri:handle_fallback(
      #{mnesia => fun() -> get_in_mnesia(Name) end,
        khepri => fun() -> get_in_khepri(Name) end
       }).

get_in_mnesia(Name) ->
    rabbit_mnesia:dirty_read({?MNESIA_TABLE, Name}).

get_in_khepri(Name) ->
    case ets:lookup(rabbit_khepri_exchange, Name) of
        [X] -> {ok, X};
        []  -> {error, not_found}
    end.

%% -------------------------------------------------------------------
%% get_in_khepri_tx().
%% -------------------------------------------------------------------

-spec get_in_khepri_tx(ExchangeName) -> Ret when
      ExchangeName :: rabbit_exchange:name(),
      Ret :: [Exchange :: rabbit_types:exchange()].

get_in_khepri_tx(Name) ->
    case khepri_tx:get(khepri_exchange_path(Name)) of
        {ok, X} -> [X];
        _ -> []
    end.

%% -------------------------------------------------------------------
%% get_many().
%% -------------------------------------------------------------------

-spec get_many([ExchangeName]) -> [Exchange] when
      ExchangeName :: rabbit_exchange:name(),
      Exchange :: rabbit_types:exchange().
%% @doc Returns the records of the exchanges named `Name'.
%%
%% @returns a list of exchange records.
%%
%% @private

get_many(Names) when is_list(Names) ->
    rabbit_khepri:handle_fallback(
      #{mnesia => fun() -> get_many_in_mnesia(?MNESIA_TABLE, Names) end,
        khepri => fun() -> get_many_in_khepri(Names) end
       }).

get_many_in_mnesia(Table, [Name]) -> ets:lookup(Table, Name);
get_many_in_mnesia(Table, Names) when is_list(Names) ->
    %% Normally we'd call mnesia:dirty_read/1 here, but that is quite
    %% expensive for reasons explained in rabbit_mnesia:dirty_read/1.
    lists:append([ets:lookup(Table, Name) || Name <- Names]).

get_many_in_khepri(Names) when is_list(Names) ->
    lists:append([ets:lookup(rabbit_khepri_exchange, Name) || Name <- Names]).

%% -------------------------------------------------------------------
%% count().
%% -------------------------------------------------------------------

-spec count() -> Num :: integer().
%% @doc Counts the number of exchanges.
%%
%% @returns the number of exchange records.
%%
%% @private

count() ->
    rabbit_khepri:handle_fallback(
      #{mnesia => fun() -> count_in_mnesia() end,
        khepri => fun() -> count_in_khepri() end
       }).

count_in_mnesia() ->
    mnesia:table_info(?MNESIA_TABLE, size).

count_in_khepri() ->
    rabbit_khepri:count_children(khepri_exchanges_path() ++ [?KHEPRI_WILDCARD_STAR]).

%% -------------------------------------------------------------------
%% update().
%% -------------------------------------------------------------------

-spec update(ExchangeName, UpdateFun) -> ok when
      ExchangeName :: rabbit_exchange:name(),
      UpdateFun :: fun((Exchange) -> Exchange).
%% @doc Updates an existing exchange record using the result of
%% `UpdateFun'.
%%
%% @returns the updated exchange record if the record existed and the
%% update succeeded. It returns `not_found' if the transaction fails.
%%
%% @private

update(XName, Fun) ->
    rabbit_khepri:handle_fallback(
      #{mnesia => fun() -> update_in_mnesia(XName, Fun) end,
        khepri => fun() -> update_in_khepri(XName, Fun) end
       }).

update_in_mnesia(XName, Fun) ->
    rabbit_mnesia:execute_mnesia_transaction(
      fun() ->
              _ = update_in_mnesia_tx(XName, Fun),
              ok
      end).

-spec update_in_mnesia_tx(ExchangeName, UpdateFun) -> Ret when
      ExchangeName :: rabbit_exchange:name(),
      Exchange :: rabbit_types:exchange(),
      UpdateFun :: fun((Exchange) -> Exchange),
      Ret :: not_found | Exchange.

update_in_mnesia_tx(Name, Fun) ->
    Table = {?MNESIA_TABLE, Name},
    case mnesia:wread(Table) of
        [X] -> X1 = Fun(X),
               set_in_mnesia_tx(X1);
        [] -> not_found
    end.

set_in_mnesia_tx(X = #exchange{durable = true}) ->
    mnesia:write(rabbit_durable_exchange, X#exchange{decorators = undefined},
                 write),
    set_ram_in_mnesia_tx(X);
set_in_mnesia_tx(X = #exchange{durable = false}) ->
    set_ram_in_mnesia_tx(X).

set_ram_in_mnesia_tx(X) ->
    X1 = rabbit_exchange_decorator:set(X),
    ok = mnesia:write(?MNESIA_TABLE, X1, write),
    X1.

update_in_khepri(XName, Fun) ->
    Path = khepri_exchange_path(XName),
    Ret1 = rabbit_khepri:adv_get(Path),
    case Ret1 of
        {ok, #{data := X, payload_version := Vsn}} ->
            X1 = Fun(X),
            UpdatePath =
                khepri_path:combine_with_conditions(
                  Path, [#if_payload_version{version = Vsn}]),
            Ret2 = rabbit_khepri:put(UpdatePath, X1),
            case Ret2 of
                ok ->
                    ok;
                {error, {khepri, mismatching_node, _}} ->
                    update_in_khepri(XName, Fun);
                {error, {khepri, node_not_found, _}} ->
                    ok;
                {error, _} = Error ->
                    Error
            end;
        {error, {khepri, node_not_found, _}} ->
            ok;
        {error, _} = Error ->
            Error
    end.

%% -------------------------------------------------------------------
%% update_in_khepri_tx().
%% -------------------------------------------------------------------

-spec update_in_khepri_tx(ExchangeName, UpdateFun) -> Ret when
      ExchangeName :: rabbit_exchange:name(),
      Exchange :: rabbit_types:exchange(),
      UpdateFun :: fun((Exchange) -> Exchange),
      Ret :: not_found | Exchange.

update_in_khepri_tx(Name, Fun) ->
    Path = khepri_exchange_path(Name),
    case khepri_tx:get(Path) of
        {ok, X} ->
            X1 = Fun(X),
            ok = khepri_tx:put(Path, X1),
            X1;
        _ -> not_found
    end.

%% -------------------------------------------------------------------
%% create_or_get().
%% -------------------------------------------------------------------

-spec create_or_get(Exchange) -> Ret when
      Exchange :: rabbit_types:exchange(),
      Ret :: {new, Exchange} | {existing, Exchange} | {error, any()}.
%% @doc Writes an exchange record if it doesn't exist already or returns
%% the existing one.
%%
%% @returns the existing record if there is one in the database already, or
%% the newly created record.
%%
%% @private

create_or_get(X) ->
    rabbit_khepri:handle_fallback(
      #{mnesia => fun() -> create_or_get_in_mnesia(X) end,
        khepri => fun() -> create_or_get_in_khepri(X) end
       }).

create_or_get_in_mnesia(#exchange{name = XName} = X) ->
    rabbit_mnesia:execute_mnesia_transaction(
      fun() ->
              case mnesia:wread({?MNESIA_TABLE, XName}) of
                  [] ->
                      {new, set_in_mnesia_tx(X)};
                  [ExistingX] ->
                      {existing, ExistingX}
              end
      end).

create_or_get_in_khepri(#exchange{name = XName} = X) ->
    Path = khepri_exchange_path(XName),
    case rabbit_khepri:create(Path, X) of
        ok ->
            {new, X};
        {error, {khepri, mismatching_node, #{node_props := #{data := ExistingX}}}} ->
            {existing, ExistingX}
    end.

%% -------------------------------------------------------------------
%% set().
%% -------------------------------------------------------------------

-spec set([Exchange]) -> ok when
      Exchange :: rabbit_types:exchange().
%% @doc Writes the exchange records.
%%
%% @returns ok.
%%
%% @private

set(Xs) ->
    rabbit_khepri:handle_fallback(
      #{mnesia => fun() -> set_in_mnesia(Xs) end,
        khepri => fun() -> set_in_khepri(Xs) end
       }).

set_in_mnesia(Xs) when is_list(Xs) ->
    rabbit_mnesia:execute_mnesia_transaction(
      fun () ->
              [mnesia:write(rabbit_durable_exchange, X, write) || X <- Xs]
      end),
    ok.

set_in_khepri(Xs) when is_list(Xs) ->
    rabbit_khepri:transaction(
      fun() ->
              [set_in_khepri_tx(X) || X <- Xs]
      end, rw),
    ok.

set_in_khepri_tx(X) ->
    Path = khepri_exchange_path(X#exchange.name),
    ok = khepri_tx:put(Path, X),
    X.

%% -------------------------------------------------------------------
%% peek_serial().
%% -------------------------------------------------------------------

-spec peek_serial(ExchangeName) -> Serial when
      ExchangeName :: rabbit_exchange:name(),
      Serial :: integer().
%% @doc Returns the next serial number without increasing it.
%%
%% @returns the next serial number
%%
%% @private

peek_serial(XName) ->
    rabbit_khepri:handle_fallback(
      #{mnesia => fun() -> peek_serial_in_mnesia(XName) end,
        khepri => fun() -> peek_serial_in_khepri(XName) end
       }).

peek_serial_in_mnesia(XName) ->
    rabbit_mnesia:execute_mnesia_transaction(
      fun() ->
              peek_serial_in_mnesia_tx(XName, read)
      end).

peek_serial_in_mnesia_tx(XName, LockType) ->
    case mnesia:read(?MNESIA_SERIAL_TABLE, XName, LockType) of
        [#exchange_serial{next = Serial}]  -> Serial;
        _                                  -> 1
    end.

peek_serial_in_khepri(XName) ->
    Path = khepri_exchange_serial_path(XName),
    case rabbit_khepri:get(Path) of
        {ok, Serial} ->
            Serial;
        _ ->
            1
    end.

%% -------------------------------------------------------------------
%% next_serial().
%% -------------------------------------------------------------------

-spec next_serial(ExchangeName) -> Serial when
      ExchangeName :: rabbit_exchange:name(),
      Serial :: integer().
%% @doc Returns the next serial number and increases it.
%%
%% @returns the next serial number
%%
%% @private

next_serial(XName) ->
    rabbit_khepri:handle_fallback(
      #{mnesia => fun() -> next_serial_in_mnesia(XName) end,
        khepri => fun() -> next_serial_in_khepri(XName) end
       }).

next_serial_in_mnesia(XName) ->
    rabbit_mnesia:execute_mnesia_transaction(fun() ->
                                                   next_serial_in_mnesia_tx(XName)
                                           end).

-spec next_serial_in_mnesia_tx(ExchangeName) -> Serial when
      ExchangeName :: rabbit_exchange:name(),
      Serial :: integer().

next_serial_in_mnesia_tx(XName) ->
    Serial = peek_serial_in_mnesia_tx(XName, write),
    ok = mnesia:write(?MNESIA_SERIAL_TABLE,
                      #exchange_serial{name = XName, next = Serial + 1}, write),
    Serial.

next_serial_in_khepri(XName) ->
    %% Just storing the serial number is enough, no need to keep #exchange_serial{}
    Path = khepri_exchange_serial_path(XName),
    Ret1 = rabbit_khepri:adv_get(Path),
    case Ret1 of
        {ok, #{data := Serial,
               payload_version := Vsn}} ->
            UpdatePath =
                khepri_path:combine_with_conditions(
                  Path, [#if_payload_version{version = Vsn}]),
            case rabbit_khepri:put(UpdatePath, Serial + 1) of
                ok ->
                    Serial;
                {error, {khepri, mismatching_node, _}} ->
                    next_serial_in_khepri(XName);
                Err ->
                    Err
            end;
        _ ->
            Serial = 1,
            ok = rabbit_khepri:put(Path, Serial + 1),
            Serial
    end.

-spec next_serial_in_khepri_tx(Exchange) -> Serial when
      Exchange :: rabbit_types:exchange(),
      Serial :: integer().

next_serial_in_khepri_tx(#exchange{name = XName}) ->
    Path = khepri_exchange_serial_path(XName),
    Serial = case khepri_tx:get(Path) of
                 {ok, Serial0} -> Serial0;
                 _ -> 1
             end,
    ok = khepri_tx:put(Path, Serial + 1),
    Serial.

%% -------------------------------------------------------------------
%% delete().
%% -------------------------------------------------------------------

-spec delete(ExchangeName, IfUnused) -> Ret when
      ExchangeName :: rabbit_exchange:name(),
      IfUnused :: boolean(),
      Exchange :: rabbit_types:exchange(),
      Binding :: rabbit_types:binding(),
      Deletions :: dict:dict(),
      Ret :: {error, not_found} | {error, in_use} | {deleted, Exchange, [Binding], Deletions}.
%% @doc Deletes an exchange record from the database. If `IfUnused' is set
%% to `true', it is only deleted when there are no bindings present on the
%% exchange.
%%
%% @returns an error if the exchange does not exist or a tuple with the exchange,
%% bindings and deletions. Bindings need to be processed on the same transaction, and
%% are later used to generate notifications. Probably shouldn't be here, but not sure
%% how to split it while keeping it atomic. Maybe something about deletions could be
%% handled outside of the transaction.
%%
%% @private

delete(XName, IfUnused) ->
    rabbit_khepri:handle_fallback(
      #{mnesia => fun() -> delete_in_mnesia(XName, IfUnused) end,
        khepri => fun() -> delete_in_khepri(XName, IfUnused) end
       }).

delete_in_mnesia(XName, IfUnused) ->
    DeletionFun = case IfUnused of
                      true  -> fun conditional_delete_in_mnesia/2;
                      false -> fun unconditional_delete_in_mnesia/2
                  end,
    rabbit_mnesia:execute_mnesia_transaction(
      fun() ->
              case mnesia:wread({?MNESIA_TABLE, XName}) of
                  [X] -> DeletionFun(X, false);
                  [] -> {error, not_found}
              end
      end).

conditional_delete_in_mnesia(X = #exchange{name = XName}, OnlyDurable) ->
    case rabbit_db_binding:has_for_source_in_mnesia(XName) of
        false  -> delete_in_mnesia(X, OnlyDurable, false);
        true   -> {error, in_use}
    end.

unconditional_delete_in_mnesia(X, OnlyDurable) ->
    delete_in_mnesia(X, OnlyDurable, true).

-spec delete_in_mnesia(Exchange, OnlyDurable, RemoveBindingsForSource) -> Ret when
      Exchange :: rabbit_types:exchange(),
      OnlyDurable :: boolean(),
      RemoveBindingsForSource :: boolean(),
      Exchange :: rabbit_types:exchange(),
      Binding :: rabbit_types:binding(),
      Deletions :: dict:dict(),
      Ret :: {error, not_found} | {error, in_use} | {deleted, Exchange, [Binding], Deletions}.
delete_in_mnesia(X = #exchange{name = XName}, OnlyDurable, RemoveBindingsForSource) ->
    ok = mnesia:delete({?MNESIA_TABLE, XName}),
    mnesia:delete({?MNESIA_DURABLE_TABLE, XName}),
    rabbit_db_binding:delete_all_for_exchange_in_mnesia(
      X, OnlyDurable, RemoveBindingsForSource).

delete_in_khepri(XName, IfUnused) ->
    DeletionFun = case IfUnused of
                      true  -> fun conditional_delete_in_khepri/2;
                      false -> fun unconditional_delete_in_khepri/2
                  end,
    rabbit_khepri:transaction(
      fun() ->
              case khepri_tx:get(khepri_exchange_path(XName)) of
                  {ok, X} -> DeletionFun(X, false);
                  _ -> {error, not_found}
              end
      end, rw).

conditional_delete_in_khepri(X = #exchange{name = XName}, OnlyDurable) ->
    case rabbit_db_binding:has_for_source_in_khepri(XName) of
        false  -> delete_in_khepri(X, OnlyDurable, false);
        true   -> {error, in_use}
    end.

unconditional_delete_in_khepri(X, OnlyDurable) ->
    delete_in_khepri(X, OnlyDurable, true).

delete_in_khepri(X = #exchange{name = XName}, OnlyDurable, RemoveBindingsForSource) ->
    ok = khepri_tx:delete(khepri_exchange_path(XName)),
    rabbit_db_binding:delete_all_for_exchange_in_khepri(X, OnlyDurable, RemoveBindingsForSource).

%% -------------------------------------------------------------------
%% delete_serial().
%% -------------------------------------------------------------------

-spec delete_serial(ExchangeName) -> ok when
      ExchangeName :: rabbit_exchange:name().
%% @doc Deletes an exchange serial record from the database.
%%
%% @returns ok
%%
%% @private

delete_serial(XName) ->
    rabbit_khepri:handle_fallback(
      #{mnesia => fun() -> delete_serial_in_mnesia(XName) end,
        khepri => fun() -> delete_serial_in_khepri(XName) end
       }).

delete_serial_in_mnesia(XName) ->
    rabbit_mnesia:execute_mnesia_transaction(
      fun() ->
              mnesia:delete({?MNESIA_SERIAL_TABLE, XName})
      end).

delete_serial_in_khepri(XName) ->
    Path = khepri_exchange_serial_path(XName),
    ok = rabbit_khepri:delete(Path).

%% -------------------------------------------------------------------
%% recover().
%% -------------------------------------------------------------------

-spec recover(VHostName) -> [Exchange] when
      Exchange :: rabbit_types:exchange(),
      VHostName :: vhost:name().
%% @doc Recovers all exchanges for a given vhost
%%
%% @returns ok
%%
%% @private

recover(VHost) ->
    rabbit_khepri:handle_fallback(
      #{mnesia => fun() -> recover_in_mnesia(VHost) end,
        khepri => fun() -> recover_in_khepri(VHost) end
       }).

recover_in_mnesia(VHost) ->
    rabbit_mnesia:table_filter(
      fun (#exchange{name = XName}) ->
              XName#resource.virtual_host =:= VHost andalso
                  mnesia:read({?MNESIA_TABLE, XName}) =:= []
      end,
      fun (X, true) ->
              X;
          (X, false) ->
              X1 = rabbit_mnesia:execute_mnesia_transaction(
                     fun() -> set_ram_in_mnesia_tx(X) end),
              Serial = rabbit_exchange:serial(X1),
              rabbit_exchange:callback(X1, create, Serial, [X1])
      end,
      ?MNESIA_DURABLE_TABLE).

recover_in_khepri(VHost) ->
    %% Transient exchanges are deprecated in Khepri, all exchanges are recovered
    %% Node boot and recovery should hang until the data is ready.
    %% Recovery needs to wait until progress can be done, as it
    %% cannot be skipped and stopping the node is not an option -
    %% the next boot most likely would behave the same way.
    %% Any other request stays with the default timeout, currently 30s.
    Exchanges0 = rabbit_db:list_in_khepri(khepri_exchanges_path() ++ [VHost, rabbit_khepri:if_has_data_wildcard()],
                                #{timeout => infinity}),
    Exchanges = [rabbit_exchange_decorator:set(X) || X <- Exchanges0],

    rabbit_khepri:transaction(
      fun() ->
              [_ = set_in_khepri_tx(X) || X <- Exchanges]
      end, rw, #{timeout => infinity}),
    %% TODO once mnesia is gone, this callback should go back to `rabbit_exchange`
    [begin
         Serial = rabbit_exchange:serial(X),
         rabbit_exchange:callback(X, create, Serial, [X])
     end || X <- Exchanges],
    Exchanges.

%% -------------------------------------------------------------------
%% match().
%% -------------------------------------------------------------------

-spec match(Pattern) -> Ret when
      Pattern :: #exchange{},
      Exchange :: rabbit_types:exchange(),
      Ret :: [Exchange] | {error, Reason :: any()}.
%% @doc Returns all exchanges that match a given pattern
%%
%% @returns a list of exchange records
%%
%% @private

match(Pattern) ->
    rabbit_khepri:handle_fallback(
      #{mnesia => fun() -> match_in_mnesia(Pattern) end,
        khepri => fun() -> match_in_khepri(Pattern) end
       }).

match_in_mnesia(Pattern) ->
    case mnesia:transaction(
           fun() ->
                   mnesia:match_object(?MNESIA_TABLE, Pattern, read)
           end) of
        {atomic, Xs} -> Xs;
        {aborted, Err} -> {error, Err}
    end.

match_in_khepri(Pattern0) ->
    Pattern = #if_data_matches{pattern = Pattern0},
    rabbit_db:list_in_khepri(khepri_exchanges_path() ++ [?KHEPRI_WILDCARD_STAR, Pattern]).

%% -------------------------------------------------------------------
%% exists().
%% -------------------------------------------------------------------

-spec exists(ExchangeName) -> Exists when
      ExchangeName :: rabbit_exchange:name(),
      Exists :: boolean().
%% @doc Indicates if the exchange named `Name' exists.
%%
%% @returns true if the exchange exists, false otherwise.
%%
%% @private

exists(Name) ->
    rabbit_khepri:handle_fallback(
      #{mnesia => fun() -> exists_in_mnesia(Name) end,
        khepri => fun() -> exists_in_khepri(Name) end
       }).

exists_in_mnesia(Name) ->
    ets:member(?MNESIA_TABLE, Name).

exists_in_khepri(Name) ->
    rabbit_khepri:exists(khepri_exchange_path(Name)).

%% -------------------------------------------------------------------
%% clear().
%% -------------------------------------------------------------------

-spec clear() -> ok.
%% @doc Deletes all exchanges.
%%
%% @private

clear() ->
    rabbit_khepri:handle_fallback(
      #{mnesia => fun() -> clear_in_mnesia() end,
        khepri => fun() -> clear_in_khepri() end
       }).

clear_in_mnesia() ->
    {atomic, ok} = mnesia:clear_table(?MNESIA_TABLE),
    {atomic, ok} = mnesia:clear_table(?MNESIA_DURABLE_TABLE),
    {atomic, ok} = mnesia:clear_table(?MNESIA_SERIAL_TABLE),
    ok.

clear_in_khepri() ->
    khepri_delete(khepri_exchanges_path()),
    khepri_delete(khepri_exchange_serials_path()).

khepri_delete(Path) ->
    case rabbit_khepri:delete(Path) of
        ok -> ok;
        Error -> throw(Error)
    end.

%% -------------------------------------------------------------------
%% maybe_auto_delete_in_mnesia().
%% -------------------------------------------------------------------

-spec maybe_auto_delete_in_mnesia(ExchangeName, boolean()) -> Ret when
      ExchangeName :: rabbit_exchange:name(),
      Exchange :: rabbit_types:exchange(),
      Deletions :: rabbit_binding:deletions(),
      Ret ::  {'not_deleted', 'undefined' | Exchange} |
              {'deleted', Exchange, Deletions}.
maybe_auto_delete_in_mnesia(XName, OnlyDurable) ->
    case mnesia:read({case OnlyDurable of
                          true  -> ?MNESIA_DURABLE_TABLE;
                          false -> ?MNESIA_TABLE
                      end, XName}) of
        []  -> {not_deleted, undefined};
        [#exchange{auto_delete = false} = X] -> {not_deleted, X};
        [#exchange{auto_delete = true} = X] ->
            case conditional_delete_in_mnesia(X, OnlyDurable) of
                {error, in_use}             -> {not_deleted, X};
                {deleted, X, [], Deletions} -> {deleted, X, Deletions}
            end
    end.

%% -------------------------------------------------------------------
%% maybe_auto_delete_in_khepri().
%% -------------------------------------------------------------------

-spec maybe_auto_delete_in_khepri(ExchangeName, boolean()) -> Ret when
      ExchangeName :: rabbit_exchange:name(),
      Exchange :: rabbit_types:exchange(),
      Deletions :: rabbit_binding:deletions(),
      Ret ::  {'not_deleted', 'undefined' | Exchange} |
              {'deleted', Exchange, Deletions}.

maybe_auto_delete_in_khepri(XName, OnlyDurable) ->
    case khepri_tx:get(khepri_exchange_path(XName)) of
        {ok, #exchange{auto_delete = false} = X} ->
            {not_deleted, X};
        {ok, #exchange{auto_delete = true} = X} ->
            case conditional_delete_in_khepri(X, OnlyDurable) of
                {error, in_use}             -> {not_deleted, X};
                {deleted, X, [], Deletions} -> {deleted, X, Deletions}
            end;
        {error, _} ->
            {not_deleted, undefined}
    end.

%% -------------------------------------------------------------------
%% Khepri paths
%% -------------------------------------------------------------------

khepri_exchanges_path() ->
    [?MODULE, exchanges].

khepri_exchange_path(#resource{virtual_host = VHost, name = Name}) ->
    [?MODULE, exchanges, VHost, Name].

khepri_exchange_serials_path() ->
    [?MODULE, exchange_serials].

khepri_exchange_serial_path(#resource{virtual_host = VHost, name = Name}) ->
    [?MODULE, exchange_serials, VHost, Name].

%% -------------------------------------------------------------------
%% path().
%% -------------------------------------------------------------------

-spec path(ExchangeName) -> Path when
      ExchangeName :: rabbit_exchange:name(),
      Path :: khepri_path:path().

path(Name) ->
    khepri_exchange_path(Name).
