%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_db_exchange).

-include_lib("khepri/include/khepri.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").

-include("include/rabbit_khepri.hrl").

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
         delete_all/1,
         delete_serial/1,
         recover/1,
         match/1,
         exists/1
        ]).

%% Used by other rabbit_db_* modules
-export([
         maybe_auto_delete_in_khepri/2,
         next_serial_in_khepri_tx/1,
         delete_in_khepri/3,
         get_in_khepri_tx/1,
         update_in_khepri_tx/2,
         clear_exchanges_in_khepri/0,
         clear_exchange_serials_in_khepri/0
         ]).

%% For testing
-export([clear/0]).

-export([
         khepri_exchange_path/1, khepri_exchange_path/2,
         khepri_exchange_serial_path/1, khepri_exchange_serial_path/2
        ]).

-define(KHEPRI_PROJECTION, rabbit_khepri_exchange).

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
    Path = khepri_exchange_path(?KHEPRI_WILDCARD_STAR, #if_has_data{}),
    rabbit_db:list_in_khepri(Path).

-spec get_all(VHostName) -> [Exchange] when
      VHostName :: vhost:name(),
      Exchange :: rabbit_types:exchange().
%% @doc Returns all exchange records in the given virtual host.
%%
%% @returns the list of exchange records.
%%
%% @private

get_all(VHost) ->
    Path = khepri_exchange_path(VHost, #if_has_data{}),
    rabbit_db:list_in_khepri(Path).

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
    get_all().

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
    try
        ets:foldr(
          fun(#exchange{name = Name}, Acc) ->
                  [Name | Acc]
          end, [], ?KHEPRI_PROJECTION)
    catch
        error:badarg ->
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
    try ets:lookup(?KHEPRI_PROJECTION, Name) of
        [X] -> {ok, X};
        []  -> {error, not_found}
    catch
        error:badarg ->
            {error, not_found}
    end.

%% -------------------------------------------------------------------
%% get_in_khepri_tx().
%% -------------------------------------------------------------------

-spec get_in_khepri_tx(ExchangeName) -> Ret when
      ExchangeName :: rabbit_exchange:name(),
      Ret :: [Exchange :: rabbit_types:exchange()].

get_in_khepri_tx(Name) ->
    Path = khepri_exchange_path(Name),
    case khepri_tx:get(Path) of
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
    try
        lists:append([ets:lookup(?KHEPRI_PROJECTION, Name) || Name <- Names])
    catch
        error:badarg ->
            []
    end.

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
    try
        ets:info(?KHEPRI_PROJECTION, size)
    catch
        error:badarg ->
            0
    end.

%% -------------------------------------------------------------------
%% update().
%% -------------------------------------------------------------------

-spec update(ExchangeName, UpdateFun) -> Ret when
      ExchangeName :: rabbit_exchange:name(),
      UpdateFun :: fun((Exchange) -> Exchange),
      Ret :: ok | rabbit_khepri:timeout_error().
%% @doc Updates an existing exchange record using the result of
%% `UpdateFun'.
%%
%% @returns the updated exchange record if the record existed and the
%% update succeeded. It returns `not_found' if the transaction fails.
%%
%% @private

update(XName, Fun) ->
    Path = khepri_exchange_path(XName),
    Ret1 = rabbit_khepri:adv_get(Path),
    case Ret1 of
        {ok, #{Path := #{data := X, payload_version := Vsn}}} ->
            X1 = Fun(X),
            UpdatePath =
                khepri_path:combine_with_conditions(
                  Path, [#if_payload_version{version = Vsn}]),
            Ret2 = rabbit_khepri:put(UpdatePath, X1),
            case Ret2 of
                ok ->
                    ok;
                {error, {khepri, mismatching_node, _}} ->
                    update(XName, Fun);
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
      Ret :: {new, Exchange} |
             {existing, Exchange} |
             rabbit_khepri:timeout_error().
%% @doc Writes an exchange record if it doesn't exist already or returns
%% the existing one.
%%
%% @returns the existing record if there is one in the database already, or
%% the newly created record.
%%
%% @private

create_or_get(#exchange{name = XName} = X) ->
    Path0 = khepri_exchange_path(XName),
    Path1 = khepri_path:combine_with_conditions(
              Path0, [#if_any{conditions =
                              [#if_node_exists{exists = false},
                               #if_has_payload{has_payload = false}]}]),
    case rabbit_khepri:put(Path1, X) of
        ok ->
            {new, X};
        {error, {khepri, mismatching_node, #{node_props := #{data := ExistingX}}}} ->
            {existing, ExistingX};
        {error, timeout} = Err ->
            Err
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
    Serial = rabbit_khepri:transaction(
               fun() ->
                       next_serial_in_khepri_tx(XName)
               end, rw),
    Serial.

-spec next_serial_in_khepri_tx(Exchange) -> Serial when
      Exchange :: rabbit_types:exchange() | rabbit_exchange:name(),
      Serial :: integer().

next_serial_in_khepri_tx(#exchange{name = XName}) ->
    next_serial_in_khepri_tx(XName);
next_serial_in_khepri_tx(XName) ->
    Path = khepri_exchange_serial_path(XName),
    Serial = case khepri_tx:get(Path) of
                 {ok, Serial0} -> Serial0;
                 _ -> 1
             end,
    %% Just storing the serial number is enough, no need to keep #exchange_serial{}
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
      Deletions :: rabbit_binding:deletions(),
      Ret :: {deleted, Exchange, [Binding], Deletions} |
             {error, not_found} |
             {error, in_use} |
             rabbit_khepri:timeout_error().
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
%% delete_all().
%% -------------------------------------------------------------------

-spec delete_all(VHostName) -> Ret when
      VHostName :: vhost:name(),
      Deletions :: rabbit_binding:deletions(),
      Ret :: {ok, Deletions}.
%% @doc Deletes all exchanges for a given vhost.
%%
%% @returns an `{ok, Deletions}' tuple containing the {@link
%% rabbit_binding:deletions()} caused by deleting the exchanges under the given
%% vhost.
%%
%% @private

delete_all(VHostName) ->
    rabbit_khepri:transaction(
      fun() ->
              delete_all_in_khepri_tx(VHostName)
      end, rw, #{timeout => infinity}).

delete_all_in_khepri_tx(VHostName) ->
    Pattern = khepri_exchange_path(VHostName, ?KHEPRI_WILDCARD_STAR),
    {ok, NodeProps} = khepri_tx_adv:delete_many(Pattern),
    Deletions =
    maps:fold(
      fun(Path, Props, Deletions) ->
              case {Path, Props} of
                  {?RABBITMQ_KHEPRI_EXCHANGE_PATH(VHostName, _),
                   #{data := X}} ->
                      {deleted,
                       #exchange{name = XName}, Bindings, XDeletions} =
                        rabbit_db_binding:delete_all_for_exchange_in_khepri(
                          X, false, true),
                      Deletions1 = rabbit_binding:add_deletion(
                                     XName, X, deleted, Bindings, XDeletions),
                      rabbit_binding:combine_deletions(Deletions, Deletions1);
                  {_, _} ->
                      Deletions
              end
      end, rabbit_binding:new_deletions(), NodeProps),
    {ok, Deletions}.

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
%% @returns a list of exchange records
%%
%% @private

recover(VHost) ->
    %% Transient exchanges are deprecated in Khepri, all exchanges are recovered
    %% Node boot and recovery should hang until the data is ready.
    %% Recovery needs to wait until progress can be done, as it
    %% cannot be skipped and stopping the node is not an option -
    %% the next boot most likely would behave the same way.
    %% Any other request stays with the default timeout, currently 30s.
    Path = khepri_exchange_path(VHost, #if_has_data{}),
    Exchanges0 = rabbit_db:list_in_khepri(Path, #{timeout => infinity}),
    Exchanges = [rabbit_exchange_decorator:set(X) || X <- Exchanges0],

    rabbit_khepri:transaction(
      fun() ->
              [_ = set_in_khepri_tx(X) || X <- Exchanges]
      end, rw, #{timeout => infinity}),
    Exchanges.

%% -------------------------------------------------------------------
%% match().
%% -------------------------------------------------------------------

-spec match(Pattern) -> Ret when
      Pattern :: #exchange{},
      Exchange :: rabbit_types:exchange(),
      Ret :: [Exchange].
%% @doc Returns all exchanges that match a given pattern
%%
%% @returns a list of exchange records
%%
%% @private

match(Pattern) ->
    Pattern1 = #if_data_matches{pattern = Pattern},
    Path = khepri_exchange_path(?KHEPRI_WILDCARD_STAR, Pattern1),
    rabbit_db:list_in_khepri(Path).

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
    try
        ets:member(?KHEPRI_PROJECTION, Name)
    catch
        error:badarg ->
            false
    end.

%% -------------------------------------------------------------------
%% clear().
%% -------------------------------------------------------------------

-spec clear() -> ok.
%% @doc Deletes all exchanges.
%%
%% @private

clear() ->
    clear_exchanges_in_khepri(),
    clear_exchange_serials_in_khepri().

clear_exchanges_in_khepri() ->
    Path = khepri_exchange_path(?KHEPRI_WILDCARD_STAR, ?KHEPRI_WILDCARD_STAR),
    khepri_delete(Path).

clear_exchange_serials_in_khepri() ->
    Path = khepri_exchange_serial_path(
             ?KHEPRI_WILDCARD_STAR, ?KHEPRI_WILDCARD_STAR),
    khepri_delete(Path).

khepri_delete(Path) ->
    case rabbit_khepri:delete_many(Path) of
        ok -> ok;
        Error -> throw(Error)
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

khepri_exchange_path(#resource{virtual_host = VHost, name = Name}) ->
    khepri_exchange_path(VHost, Name).

khepri_exchange_path(VHost, Name)
  when ?IS_KHEPRI_PATH_CONDITION(VHost) andalso
       ?IS_KHEPRI_PATH_CONDITION(Name) ->
    ?RABBITMQ_KHEPRI_EXCHANGE_PATH(VHost, Name).

khepri_exchange_serial_path(#resource{virtual_host = VHost, name = Name}) ->
    khepri_exchange_serial_path(VHost, Name).

khepri_exchange_serial_path(VHost, Name)
  when ?IS_KHEPRI_PATH_CONDITION(VHost) andalso
       ?IS_KHEPRI_PATH_CONDITION(Name) ->
    ?RABBITMQ_KHEPRI_EXCHANGE_SERIAL_PATH(VHost, Name).
