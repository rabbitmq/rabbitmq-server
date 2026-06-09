%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_db_queue).

-include_lib("khepri/include/khepri.hrl").
-include_lib("rabbit_common/include/logging.hrl").
-include_lib("rabbit_common/include/rabbit.hrl").
-include("amqqueue.hrl").

-include("include/rabbit_khepri.hrl").
-include_lib("kernel/include/logger.hrl").

-compile({no_auto_import, [get/1]}).

-export([
         get/1,
         get_targets/1,
         get_all/0,
         get_all/1,
         get_all_by_type/1,
         get_all_by_type_and_vhost/2,
         get_all_by_type_and_node/3,
         list/0,
         count/0,
         count/1,
         create_or_get/1,
         set/1,
         delete/2,
         delete_if/3,
         update/2,
         update/3,
         update_decorators/2,
         exists/1,
         foreach/2
        ]).

%% TODO: These can be replaced with the plain get_all* functions.
-export([
         get_all_durable/0,
         get_all_durable_by_type/1,
         filter_all_durable/1,
         update_durable/2,
         get_durable/1,
         get_many_durable/1,
         consistent_exists/1
        ]).

%% Used by on_node_up and on_node_down.
-export([list_transient/0,
         foreach_transient/1,
         delete_transient/1]).

%% TODO: This is a no-op, consider removing.
-export([set_dirty/1]).

%% Used by other rabbit_db_* modules
-export([
         update_in_khepri_tx/2,
         get_in_khepri_tx/1
        ]).

%% Used for the `tie_binding_to_dest_with_keep_while_cond' feature flag.
-export([tie_binding_to_dest_with_keep_while_cond_enable/1]).

%% For testing
-export([clear/0]).

-export([khepri_queue_path/1, khepri_queue_path/2]).

-dialyzer({nowarn_function, [foreach_transient/1]}).

-define(KHEPRI_PROJECTION, rabbit_khepri_queue).
-define(KHEPRI_TARGET_PROJECTION, rabbit_khepri_queue_target).

%% -------------------------------------------------------------------
%% get_all().
%% -------------------------------------------------------------------

-spec get_all() -> [Queue] when
      Queue :: amqqueue:amqqueue().

%% @doc Returns all queue records.
%%
%% @returns the list of all queue records.
%%
%% @private

get_all() ->
    list_with_possible_retry(
      fun() ->
              try
                  ets:tab2list(?KHEPRI_PROJECTION)
              catch
                  error:badarg ->
                      []
              end
      end).

-spec get_all(VHostName) -> [Queue] when
      VHostName :: vhost:name(),
      Queue :: amqqueue:amqqueue().

%% @doc Gets all queues belonging to the given virtual host
%%
%% @returns a list of queue records.
%%
%% @private

get_all(VHostName) ->
    list_with_possible_retry(
      fun() ->
              try
                  Pattern = amqqueue:pattern_match_on_name(rabbit_misc:r(VHostName, queue)),
                  ets:match_object(?KHEPRI_PROJECTION, Pattern)
              catch
                  error:badarg ->
                      []
              end
      end).

%% -------------------------------------------------------------------
%% get_all_durable().
%% -------------------------------------------------------------------

-spec get_all_durable() -> [Queue] when
      Queue :: amqqueue:amqqueue().

%% @doc Returns all durable queue records.
%%
%% @returns a list of queue records.
%%
%% @private

get_all_durable() ->
    list_with_possible_retry(
      fun() ->
              try
                  Pattern = amqqueue:pattern_match_on_durable(true),
                  ets:match_object(?KHEPRI_PROJECTION, Pattern)
              catch
                  error:badarg ->
                      []
              end
      end).

-spec get_all_durable_by_type(Type) -> [Queue] when
      Type :: atom(),
      Queue :: amqqueue:amqqueue().

%% @doc Gets all durable queues of the given type
%%
%% @returns a list of queue records.
%%
%% @private

get_all_durable_by_type(Type) ->
    try
        Pattern = amqqueue:pattern_match_on_type_and_durable(Type, true),
        ets:match_object(?KHEPRI_PROJECTION, Pattern)
    catch
        error:badarg ->
            []
    end.

%% -------------------------------------------------------------------
%% filter_all_durable().
%% -------------------------------------------------------------------

-spec filter_all_durable(FilterFun) -> [Queue] when
      Queue :: amqqueue:amqqueue(),
      FilterFun :: fun((Queue) -> boolean()).

%% @doc Filters all durable queues
%%
%% @returns a list of queue records.
%%
%% @private

filter_all_durable(FilterFun) ->
    try
        ets:foldl(
          fun(Q, Acc0) ->
                  case amqqueue:is_durable(Q) andalso FilterFun(Q) of
                      true -> [Q | Acc0];
                      false -> Acc0
                  end
          end,
          [], ?KHEPRI_PROJECTION)
    catch
        error:badarg ->
            []
    end.

%% -------------------------------------------------------------------
%% list().
%% -------------------------------------------------------------------

-spec list() -> [QName] when
      QName :: rabbit_amqqueue:name().

%% @doc Returns all queue names.
%%
%% @returns the list of all queue names.
%%
%% @private

list() ->
    try
        Pattern = amqqueue:pattern_match_on_name('$1'),
        ets:select(?KHEPRI_PROJECTION, [{Pattern, [], ['$1']}])
    catch
        error:badarg ->
            []
    end.

%% -------------------------------------------------------------------
%% count().
%% -------------------------------------------------------------------

-spec count() -> Count when
      Count :: integer().

%% @doc Counts the number of queues
%%
%% @returns the number of queues.
%%
%% @private

count() ->
    case ets:info(?KHEPRI_PROJECTION, size) of
        undefined ->
            %% `ets:info/2` on a table that does not exist returns `undefined`.
            0;
        Size ->
            Size
    end.

-spec count(VHostName) -> Count when
      VHostName :: vhost:name(),
      Count :: integer().

%% @doc Counts the number of queues for the given vhost
%%
%% @returns the number of queues for the given vhost
%%
%% @private

count(VHostName) ->
    try
        list_for_count(VHostName)
    catch _:Err ->
            ?LOG_ERROR("Failed to fetch number of queues in vhost ~p:~n~p",
                             [VHostName, Err]),
            0
    end.

list_for_count(VHostName) ->
    try
        Pattern = amqqueue:pattern_match_on_name(rabbit_misc:r(VHostName, queue)),
        ets:select_count(?KHEPRI_PROJECTION, [{Pattern, [], [true]}])
    catch
        error:badarg ->
            0
    end.

%% -------------------------------------------------------------------
%% delete().
%% -------------------------------------------------------------------

-spec delete(QName, OnlyDurable) -> Ret when
      QName :: rabbit_amqqueue:name(),
      OnlyDurable :: boolean(),
      Ret :: ok |
             Deletions :: rabbit_binding:deletions() |
             rabbit_khepri:timeout_error().

delete(QueueName, OnlyDurable) ->
    delete_if(QueueName, [], OnlyDurable).

-spec delete_if(QName, Conditions, OnlyDurable) -> Ret when
      QName :: rabbit_amqqueue:name(),
      Conditions :: [khepri_condition:condition()],
      OnlyDurable :: boolean(),
      Ret :: ok |
             Deletions :: rabbit_binding:deletions() |
             rabbit_khepri:timeout_error().

delete_if(QueueName, Conditions, OnlyDurable) ->
    Path = khepri_queue_path(QueueName),
    Pattern = khepri_path:combine_with_conditions(Path, Conditions),
    FeatureFlag = rabbit_feature_flags:is_enabled(
                    tie_binding_to_dest_with_keep_while_cond),
    case FeatureFlag of
        true ->
            case rabbit_khepri:adv_delete(Pattern) of
                {ok, #{Path := #{data := _}} = Deleted} ->
                    rabbit_db_binding:khepri_ret_to_deletions(
                      Deleted, OnlyDurable);
                {ok, _} ->
                    ok;
                {error, _} = Error ->
                    Error
            end;
        false ->
            rabbit_khepri:transaction(
              fun () ->
                      UsesUniformWriteRet = (
                        try
                            khepri_tx:does_api_comply_with(uniform_write_ret)
                        catch
                            error:undef ->
                                false
                        end),
                      case khepri_tx_adv:delete(Pattern) of
                          {ok, #{Path := #{data := _}}}
                            when UsesUniformWriteRet ->
                              %% we want to execute some things, as decided by
                              %% rabbit_exchange, after the transaction.
                              rabbit_db_binding:delete_for_destination_in_khepri(
                                QueueName, OnlyDurable);
                          {ok, #{data := _}}
                            when not UsesUniformWriteRet ->
                              %% we want to execute some things, as decided by
                              %% rabbit_exchange, after the transaction.
                              rabbit_db_binding:delete_for_destination_in_khepri(
                                QueueName, OnlyDurable);
                          {ok, _} ->
                              ok
                      end
              end, rw)
    end.

%% -------------------------------------------------------------------
%% get_targets().
%% -------------------------------------------------------------------

-spec get_targets(rabbit_exchange:route_return()) ->
    [amqqueue:target() | {amqqueue:target(), rabbit_exchange:route_infos()}].
get_targets(Names) ->
    lists:filtermap(fun({Name, RouteInfos})
                          when is_map(RouteInfos) ->
                            case lookup_target(Name) of
                                not_found -> false;
                                Target -> {true, {Target, RouteInfos}}
                            end;
                       (Name) ->
                            case lookup_target(Name) of
                                not_found -> false;
                                Target -> {true, Target}
                            end
                    end, Names).

lookup_target(#resource{name = NameBin} = Name) ->
    case rabbit_volatile_queue:is(NameBin) of
        true ->
            %% This queue is not stored in the database. We create it on the fly.
            case rabbit_volatile_queue:new_target(Name) of
                error -> not_found;
                Target -> Target
            end;
        false ->
            lookup_target0(Name)
    end.

lookup_target0(Name) ->
    try ets:lookup_element(?KHEPRI_TARGET_PROJECTION, Name, 2, not_found) of
        not_found ->
            not_found;
        Target ->
            amqqueue:new_target(Name, Target)
    catch
        error:badarg ->
            not_found
    end.

%% -------------------------------------------------------------------
%% get().
%% -------------------------------------------------------------------

-spec get(QName) -> Ret when
      QName :: rabbit_amqqueue:name(),
      Ret :: {ok, Queue :: amqqueue:amqqueue()} | {error, not_found}.
get(#resource{name = NameBin} = Name) ->
    case rabbit_volatile_queue:is(NameBin) of
        true ->
            case rabbit_volatile_queue:new(Name) of
                error -> {error, not_found};
                Q -> {ok, Q}
            end;
        false ->
            get1(Name)
    end.

get1(Name) ->
    try ets:lookup(?KHEPRI_PROJECTION, Name) of
        [Q] -> {ok, Q};
        []  -> {error, not_found}
    catch
        error:badarg ->
            {error, not_found}
    end.

%% -------------------------------------------------------------------
%% get_durable().
%% -------------------------------------------------------------------

-spec get_durable(QName) -> Ret when
      QName :: rabbit_amqqueue:name(),
      Ret :: {ok, Queue :: amqqueue:amqqueue()} | {error, not_found}.

get_durable(Name) ->
    case get(Name) of
        {ok, Queue} = Ret ->
            case amqqueue:is_durable(Queue) of
                true  -> Ret;
                false -> {error, not_found}
            end;
        Error ->
            Error
    end.

%% -------------------------------------------------------------------
%% get_many_durable().
%% -------------------------------------------------------------------

-spec get_many_durable([QName]) -> Ret when
      QName :: rabbit_amqqueue:name(),
      Ret :: [Queue :: amqqueue:amqqueue()].

get_many_durable(Names) when is_list(Names) ->
    try
        Queues = get_many_in_ets(?KHEPRI_PROJECTION, Names),
        [Q || Q <- Queues, amqqueue:is_durable(Q)]
    catch
        error:badarg ->
            []
    end.

get_many_in_ets(Table, Names) ->
    lists:filtermap(fun(Name) ->
                            case ets:lookup(Table, Name) of
                                [] -> false;
                                [Q] -> {true, Q}
                            end
                    end, Names).

%% -------------------------------------------------------------------
%% update().
%% -------------------------------------------------------------------

-spec update(QName, UpdateFun) -> Ret when
      QName :: rabbit_amqqueue:name(),
      Queue :: amqqueue:amqqueue(),
      UpdateFun :: fun((Queue) -> Queue),
      Ret :: Queue | not_found.
%% @doc Updates an existing queue record using `UpdateFun'.
%%
%% @private

update(QName, Fun) ->
    update(QName, Fun, #{}).

-spec update(QName, UpdateFun, Options) -> Ret when
      QName :: rabbit_amqqueue:name(),
      Queue :: amqqueue:amqqueue(),
      UpdateFun :: fun((Queue) -> NewQueue),
      NewQueue :: amqqueue:amqqueue(),
      Options :: #{timeout => timeout()},
      Ret :: Queue | not_found.
%% @doc Updates an existing queue record using `UpdateFun'.
%%
%% @private

update(QName, Fun, Options) ->
    Path = khepri_queue_path(QName),
    Ret1 = rabbit_khepri:adv_get(Path, Options),
    case Ret1 of
        {ok, #{Path := #{data := Q, payload_version := Vsn}}} ->
            UpdatePath = khepri_path:combine_with_conditions(
                           Path, [#if_payload_version{version = Vsn}]),
            Q1 = Fun(Q),
            Ret2 = rabbit_khepri:put(UpdatePath, Q1, Options),
            case Ret2 of
                ok -> Q1;
                {error, {khepri, mismatching_node, _}} ->
                    update(QName, Fun);
                Err -> Err
            end;
        _  ->
            not_found
    end.

%% -------------------------------------------------------------------
%% update_decorators().
%% -------------------------------------------------------------------

-spec update_decorators(QName, [Decorator]) -> ok when
      QName :: rabbit_amqqueue:name(),
      Decorator :: atom().
%% @doc Updates an existing queue record adding the active queue decorators.
%%
%% @private

update_decorators(QName, Decorators) ->
    %% Decorators are stored on an ETS table, so we need to query them before the transaction.
    %% Also, to verify which ones are active could lead to any kind of side-effects.
    %% Thus it needs to be done outside of the transaction.
    %% Decorators have just been calculated on `rabbit_queue_decorator:maybe_recover/1`, thus
    %% we can update them here directly.
    Path = khepri_queue_path(QName),
    Ret1 = rabbit_khepri:adv_get(Path),
    case Ret1 of
        {ok, #{Path := #{data := Q1, payload_version := Vsn}}} ->
            Q2 = amqqueue:set_decorators(Q1, Decorators),
            UpdatePath = khepri_path:combine_with_conditions(
                           Path, [#if_payload_version{version = Vsn}]),
            Ret2 = rabbit_khepri:put(UpdatePath, Q2),
            case Ret2 of
                ok -> ok;
                {error, {khepri, mismatching_node, _}} ->
                    update_decorators(QName, Decorators);
                {error, _} = Error -> Error
            end;
        _  ->
            ok
    end.

%% -------------------------------------------------------------------
%% update_durable().
%% -------------------------------------------------------------------

-spec update_durable(UpdateFun, FilterFun) -> ok when
      UpdateFun :: fun((Queue) -> any()),
      FilterFun :: fun((Queue) -> boolean()).
%% @doc Applies `UpdateFun' to all durable queue records that match `FilterFun'
%% and stores them
%%
%% @private

update_durable(UpdateFun, FilterFun) ->
    PathPattern = khepri_queue_path(
                    ?KHEPRI_WILDCARD_STAR,
                    #if_data_matches{
                       pattern = amqqueue:pattern_match_on_durable(true)}),
    %% The `FilterFun' or `UpdateFun' might attempt to do something
    %% incompatible with Khepri transactions (such as dynamic apply, sending
    %% a message, etc.), so this function cannot be written as a regular
    %% transaction. Instead we can get all queues and track their versions,
    %% update them, then apply the updates in a transaction, failing if any
    %% queue has changed since reading the queue record.
    case rabbit_khepri:adv_get_many(PathPattern) of
        {ok, Props} ->
            Updates = maps:fold(
                        fun(Path0, #{data := Q0, payload_version := Vsn}, Acc)
                            when ?is_amqqueue(Q0) ->
                                case FilterFun(Q0) of
                                    true ->
                                        Path = khepri_path:combine_with_conditions(
                                                 Path0,
                                                 [#if_payload_version{version = Vsn}]),
                                        Q = UpdateFun(Q0),
                                        [{Path, Q} | Acc];
                                    false ->
                                        Acc
                                end
                        end, [], Props),
            Res = rabbit_khepri:transaction(
                    fun() ->
                            rabbit_misc:for_each_while_ok(
                              fun({Path, Q}) -> khepri_tx:put(Path, Q) end,
                              Updates)
                    end),
            case Res of
                ok ->
                    ok;
                {error, {khepri, mismatching_node, _}} ->
                    %% One of the queues changed while attempting to update
                    %% all queues. Retry the operation.
                    update_durable(UpdateFun, FilterFun);
                {error, _} = Error ->
                    Error
            end;
        {error, _} = Error ->
            Error
    end.

%% -------------------------------------------------------------------
%% exists().
%% -------------------------------------------------------------------

-spec exists(QName) -> Exists when
      QName :: rabbit_amqqueue:name(),
      Exists :: boolean().
%% @doc Indicates if queue named `QName' exists.
%%
%% @returns true if the queue exists, false otherwise.
%%
%% @private

exists(#resource{name = NameBin} = Name) ->
    case rabbit_volatile_queue:is(NameBin) of
        true ->
            rabbit_volatile_queue:exists(Name);
        false ->
            exists1(Name)
    end.

exists1(QName) ->
    try
        ets:member(?KHEPRI_PROJECTION, QName)
    catch
        error:badarg ->
            false
    end.

%% -------------------------------------------------------------------
%% exists().
%% -------------------------------------------------------------------

-spec consistent_exists(QName) -> Exists when
      QName :: rabbit_amqqueue:name(),
      Exists :: boolean().
%% @doc Indicates if queue named `QName' exists using a consistent read.
%%
%% Just used by `rabbit_classic_queue:is_recoverable()' for transient queues.
%%
%% @returns true if the queue exists, false otherwise.
%%
%% @private

consistent_exists(QName) ->
    exists(QName).

%% -------------------------------------------------------------------
%% get_all_by_type().
%% -------------------------------------------------------------------

-spec get_all_by_type(Type) -> [Queue] when
      Type :: atom(),
      Queue :: amqqueue:amqqueue().

%% @doc Gets all queues belonging to the given type
%%
%% @returns a list of queue records.
%%
%% @private

get_all_by_type(Type) ->
    Pattern = amqqueue:pattern_match_on_type(Type),
    get_all_by_pattern(Pattern).

%% -------------------------------------------------------------------
%% get_all_by_type_and_vhost().
%% -------------------------------------------------------------------

-spec get_all_by_type_and_vhost(Type, VHost) -> [Queue] when
      Type :: atom(),
      VHost :: binary(),
      Queue :: amqqueue:amqqueue().

%% @doc Gets all queues belonging to the given type and vhost
%%
%% @returns a list of queue records.
%%
%% @private

get_all_by_type_and_vhost(Type, VHost) ->
    Pattern = amqqueue:pattern_match_on_type_and_vhost(Type, VHost),
    get_all_by_pattern(Pattern).

get_all_by_pattern(Pattern) ->
    Path = khepri_queue_path(
             ?KHEPRI_WILDCARD_STAR,
             #if_data_matches{pattern = Pattern}),
    rabbit_db:list_in_khepri(Path).

%% -------------------------------------------------------------------
%% get_all_by_type_and_node().
%% -------------------------------------------------------------------

-spec get_all_by_type_and_node(VHostName, Type, Node) -> [Queue] when
      VHostName :: vhost:name(),
      Type :: atom(),
      Node :: 'none' | atom(),
      Queue :: amqqueue:amqqueue().

%% @doc Gets all queues belonging to the given type
%%
%% @returns a list of queue records.
%%
%% @private

get_all_by_type_and_node(VHostName, Type, Node) ->
    Pattern = amqqueue:pattern_match_on_type(Type),
    Path = khepri_queue_path(VHostName, #if_data_matches{pattern = Pattern}),
    Qs = rabbit_db:list_in_khepri(Path),
    [Q || Q <- Qs, amqqueue:qnode(Q) == Node].

%% -------------------------------------------------------------------
%% create_or_get().
%% -------------------------------------------------------------------

-spec create_or_get(Queue) -> Ret when
      Queue :: amqqueue:amqqueue(),
      Ret :: {created, Queue} |
             {existing, Queue} |
             {absent, Queue, nodedown} |
             rabbit_khepri:timeout_error().
%% @doc Writes a queue record if it doesn't exist already or returns the existing one
%%
%% @returns the existing record if there is one in the database already, or the newly
%% created record.
%%
%% @private

create_or_get(Q) ->
    QueueName = amqqueue:get_name(Q),
    Path = khepri_queue_path(QueueName),
    case rabbit_khepri:adv_create(Path, Q) of
        {error, {khepri, mismatching_node, #{node_props := #{data := ExistingQ}}}} ->
            {existing, ExistingQ};
        {ok, _} ->
            {created, Q};
        Error ->
            Error
    end.

%% -------------------------------------------------------------------
%% set().
%% -------------------------------------------------------------------

-spec set(Queue) -> Ret when
      Queue :: amqqueue:amqqueue(),
      Ret :: ok | rabbit_khepri:timeout_error().
%% @doc Writes a queue record. If the queue is durable, it writes both instances:
%% durable and transient. For the durable one, it resets decorators.
%% The transient one is left as it is.
%%
%% @private

set(Q) ->
    Path = khepri_queue_path(amqqueue:get_name(Q)),
    rabbit_khepri:put(Path, Q).

%% -------------------------------------------------------------------
%% list_transient().
%% -------------------------------------------------------------------

-spec list_transient() -> {ok, Queues} | {error, any()} when
      Queues :: [amqqueue:amqqueue()].
%% @doc Applies `UpdateFun' to all transient queue records.
%%
%% @private

list_transient() ->
    try
        List = ets:match_object(
                 ?KHEPRI_PROJECTION,
                 amqqueue:pattern_match_on_durable(false)),
        {ok, List}
    catch
        error:badarg ->
            {error, {khepri_projection_missing, ?KHEPRI_WILDCARD_STAR}}
    end.

%% -------------------------------------------------------------------
%% delete_transient().
%% -------------------------------------------------------------------

-spec delete_transient(FilterFun) -> Ret when
      Queue :: amqqueue:amqqueue(),
      FilterFun :: fun((Queue) -> boolean()),
      QName :: rabbit_amqqueue:name(),
      Ret :: {[QName], [Deletions :: rabbit_binding:deletions()]}
             | rabbit_khepri:timeout_error().
%% @doc Deletes all transient queues that match `FilterFun'.
%%
%% @private

delete_transient(FilterFun) ->
    PathPattern = khepri_queue_path(
                    ?KHEPRI_WILDCARD_STAR,
                    #if_data_matches{
                       pattern = amqqueue:pattern_match_on_durable(false)}),
    %% The `FilterFun' might try to determine if the queue's process is alive.
    %% This can cause a `calling_self' exception if we use the `FilterFun'
    %% within the function passed to `khepri:fold/5' since the Khepri server
    %% process might call itself. Instead we can fetch all of the transient
    %% queues with `get_many' and then filter and fold the results outside of
    %% Khepri's Ra server process.
    case rabbit_khepri:adv_get_many(PathPattern) of
        {ok, Props} ->
            Qs = maps:fold(
                   fun(Path, #{data := Q, payload_version := Vsn}, Acc)
                       when ?is_amqqueue(Q) ->
                           case FilterFun(Q) of
                               true ->
                                   QName = amqqueue:get_name(Q),
                                   [{Path, Vsn, QName} | Acc];
                               false ->
                                   Acc
                           end
                   end, [], Props),
            do_delete_transient_queues_in_khepri(Qs, FilterFun);
        {error, _} = Error ->
            Error
    end.

do_delete_transient_queues_in_khepri([], _FilterFun) ->
    %% If there are no changes to make, avoid performing a transaction. When
    %% Khepri is in a minority this avoids a long timeout waiting for the
    %% transaction command to be processed. Otherwise it avoids appending a
    %% somewhat large transaction command to Khepri's log.
    {[], []};
do_delete_transient_queues_in_khepri(Qs, FilterFun) ->
    Res = rabbit_khepri:transaction(
            fun() ->
                    do_delete_transient_queues_in_khepri_tx(Qs, [])
            end),
    case Res of
        {ok, Items} ->
            {QNames, Deletions} = lists:unzip(Items),
            {QNames, lists:flatten(Deletions)};
        {error, {khepri, mismatching_node, _}} ->
            %% One of the queues changed while attempting to update all
            %% queues. Retry the operation.
            delete_transient(FilterFun);
        {error, _} = Error ->
            Error
    end.

do_delete_transient_queues_in_khepri_tx([], Acc) ->
    {ok, Acc};
do_delete_transient_queues_in_khepri_tx([{Path, Vsn, QName} | Rest], Acc) ->
    %% Also see `delete/2'.
    VersionedPath = khepri_path:combine_with_conditions(
                      Path, [#if_payload_version{version = Vsn}]),
    UsesUniformWriteRet = try
                              khepri_tx:does_api_comply_with(uniform_write_ret)
                          catch
                              error:undef ->
                                  false
                          end,
    case khepri_tx_adv:delete(VersionedPath) of
        {ok, #{Path := #{data := _}}} when UsesUniformWriteRet ->
            Deletions = rabbit_db_binding:delete_for_destination_in_khepri(
                          QName, false),
            Acc1 = [{QName, Deletions} | Acc],
            do_delete_transient_queues_in_khepri_tx(Rest, Acc1);
        {ok, #{data := _}} when not UsesUniformWriteRet ->
            Deletions = rabbit_db_binding:delete_for_destination_in_khepri(
                          QName, false),
            Acc1 = [{QName, Deletions} | Acc],
            do_delete_transient_queues_in_khepri_tx(Rest, Acc1);
        {ok, _} ->
            do_delete_transient_queues_in_khepri_tx(Rest, Acc);
        {error, _} = Error ->
            Error
    end.

%% -------------------------------------------------------------------
%% foreach_transient().
%% -------------------------------------------------------------------

-spec foreach_transient(UpdateFun) -> ok when
      Queue :: amqqueue:amqqueue(),
      UpdateFun :: fun((Queue) -> any()).
%% @doc Applies `UpdateFun' to all transient queue records.
%%
%% @private

foreach_transient(UpdateFun) ->
    PathPattern = khepri_queue_path(
                    ?KHEPRI_WILDCARD_STAR,
                    #if_data_matches{
                       pattern = amqqueue:pattern_match_on_durable(false)}),
    %% The `UpdateFun' might try to determine if the queue's process is alive.
    %% This can cause a `calling_self' exception if we use the `UpdateFun'
    %% within the function passed to `khepri:fold/5' since the Khepri server
    %% process might call itself. Instead we can fetch all of the transient
    %% queues with `get_many' and then filter and fold the results outside of
    %% Khepri's Ra server process.
    case rabbit_khepri:get_many(PathPattern) of
        {ok, Qs} ->
            maps:foreach(
              fun(_Path, Queue) when ?is_amqqueue(Queue) ->
                      UpdateFun(Queue)
              end, Qs);
        {error, _} = Error ->
            Error
    end.

%% -------------------------------------------------------------------
%% foreach().
%% -------------------------------------------------------------------

-spec foreach(UpdateFun, FilterFun) -> ok when
      UpdateFun :: fun((Queue) -> any()),
      FilterFun :: fun((Queue) -> boolean()).
%% @doc Applies `UpdateFun' to all queue records that match `FilterFun'.
%%
%% All queues are considered, both durable and transient.
%%
%% @private

foreach(UpdateFun, FilterFun) ->
    Path = khepri_queue_path(?KHEPRI_WILDCARD_STAR, ?KHEPRI_WILDCARD_STAR),
    case rabbit_khepri:filter(Path, fun(_, #{data := Q}) ->
                                            FilterFun(Q)
                                    end) of
        {ok, Qs} ->
            _ = [UpdateFun(Q) || Q <- maps:values(Qs)],
            ok;
        Error ->
            Error
    end.

%% -------------------------------------------------------------------
%% set_dirty().
%% -------------------------------------------------------------------

-spec set_dirty(Queue) -> ok when
      Queue :: amqqueue:amqqueue().
%% @doc Writes a transient queue record
%%
%% @private

set_dirty(_Q) ->
    ok.

%% -------------------------------------------------------------------
%% update_in_khepri_tx().
%% -------------------------------------------------------------------

-spec update_in_khepri_tx(QName, UpdateFun) -> Ret when
      QName :: rabbit_amqqueue:name(),
      Queue :: amqqueue:amqqueue(),
      UpdateFun :: fun((Queue) -> Queue),
      Ret :: Queue | not_found.

update_in_khepri_tx(Name, Fun) ->
    Path = khepri_queue_path(Name),
    case khepri_tx:get(Path) of
        {ok, Q} ->
            Q1 = Fun(Q),
            ok = khepri_tx:put(Path, Q1),
            Q1;
        _  ->
            not_found
    end.

%% -------------------------------------------------------------------
%% get_in_khepri_tx().
%% -------------------------------------------------------------------

get_in_khepri_tx(Name) ->
    case khepri_tx:get(khepri_queue_path(Name)) of
        {ok, X} -> [X];
        _ -> []
    end.

%% -------------------------------------------------------------------
%% clear().
%% -------------------------------------------------------------------

-spec clear() -> ok.
%% @doc Deletes all queues.
%%
%% @private

clear() ->
    Path = khepri_queue_path(?KHEPRI_WILDCARD_STAR, ?KHEPRI_WILDCARD_STAR),
    case rabbit_khepri:delete_many(Path) of
        ok -> ok;
        Error -> throw(Error)
    end.

%% --------------------------------------------------------------
%% Internal
%% --------------------------------------------------------------

list_with_possible_retry(Fun) ->
    %% Retry in case of an amqqueue record version mismatch.
    AmqqueueRecordVersion = amqqueue:record_version_to_use(),
    case Fun() of
        [] ->
            case khepri_tx:is_transaction() of
                true ->
                    [];
                false ->
                    case amqqueue:record_version_to_use() of
                        AmqqueueRecordVersion -> [];
                        _                     -> Fun()
                    end
            end;
        Ret ->
            Ret
    end.

tie_binding_to_dest_with_keep_while_cond_enable(
  #{feature_name := FeatureName}) ->
    ?LOG_DEBUG(
       "Feature flag `~s`: send transaction to add `keep_while` conditions",
       [FeatureName],
       #{domain => ?RMQLOG_DOMAIN_DB}),
    Ret = rabbit_khepri:transaction(
            fun() ->
                    %% Auto-delete exchange.
                    ?LOG_DEBUG(
                       "Feature flag `~s`: add `keep_while` condition to "
                       "auto-delete exchanges",
                       [FeatureName],
                       #{domain => ?RMQLOG_DOMAIN_DB}),
                    ExchangePattern = rabbit_db_exchange:khepri_exchange_path(
                                        ?KHEPRI_WILDCARD_STAR,
                                        ?KHEPRI_WILDCARD_STAR),
                    ok = khepri_tx:foreach(
                           ExchangePattern,
                           fun(ExchangePath, #{data := Exchange}) ->
                                   PutOptions = rabbit_db_exchange:put_options(
                                                  Exchange),
                                   case PutOptions =:= #{} of
                                       true ->
                                           ok;
                                       false ->
                                           ok = khepri_tx:put(
                                                  ExchangePath, Exchange,
                                                  PutOptions)
                                   end
                           end),

                    %% Bindings.
                    ?LOG_DEBUG(
                       "Feature flag `~s`: add `keep_while` condition to "
                       "bindings",
                       [FeatureName],
                       #{domain => ?RMQLOG_DOMAIN_DB}),
                    BindingPattern = rabbit_db_binding:khepri_route_path(
                                       ?KHEPRI_WILDCARD_STAR,
                                       ?KHEPRI_WILDCARD_STAR,
                                       ?KHEPRI_WILDCARD_STAR,
                                       ?KHEPRI_WILDCARD_STAR,
                                       ?KHEPRI_WILDCARD_STAR),
                    ok = khepri_tx:foreach(
                           BindingPattern,
                           fun(BindingPath, #{data := Set}) ->
                                   PutOptions = rabbit_db_binding:put_options(
                                                  BindingPath),
                                   ok = khepri_tx:put(
                                          BindingPath, Set,
                                          PutOptions)
                           end),
                    ok
            end, rw),
    ?LOG_DEBUG(
       "Feature flag `~s`: transaction return value: ~p",
       [FeatureName, Ret],
       #{domain => ?RMQLOG_DOMAIN_DB}),
    case Ret of
        {ok, ok} ->
            ok;
        Error ->
            Error
    end.

%% --------------------------------------------------------------
%% Khepri paths
%% --------------------------------------------------------------

khepri_queue_path(#resource{virtual_host = VHost, name = Name}) ->
    khepri_queue_path(VHost, Name).

khepri_queue_path(VHost, Name)
  when ?IS_KHEPRI_PATH_CONDITION(VHost) andalso
       ?IS_KHEPRI_PATH_CONDITION(Name) ->
    ?RABBITMQ_KHEPRI_QUEUE_PATH(VHost, Name).
