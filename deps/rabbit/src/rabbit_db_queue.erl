%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2023 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_db_queue).

-include_lib("rabbit_common/include/rabbit.hrl").
-include_lib("stdlib/include/qlc.hrl").
-include("amqqueue.hrl").

-export([
         get/1,
         get_many/1,
         get_all/0,
         get_all/1,
         get_all_by_type/1,
         get_all_by_type_and_node/3,
         list/0,
         count/0,
         count/1,
         create_or_get/1,
         set/1,
         set_many/1,
         delete/2,
         update/2,
         update_decorators/1,
         exists/1
        ]).

-export([
         get_all_durable/0,
         get_all_durable_by_type/1,
         filter_all_durable/1,
         update_durable/2,
         get_durable/1,
         get_many_durable/1,
         consistent_exists/1
        ]).

%% Used by on_node_up and on_node_down
-export([foreach_transient/1,
         delete_transient/1]).

%% Used only by forget all durable
-export([foreach_durable/2,
         internal_delete/3]).

-export([set_dirty/1]).

%% Used by other rabbit_db_* modules
-export([
         update_in_mnesia_tx/2,
         get_durable_in_mnesia_tx/1
        ]).

%% For testing
-export([clear/0]).

-define(MNESIA_TABLE, rabbit_queue).
-define(MNESIA_DURABLE_TABLE, rabbit_durable_queue).

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
    rabbit_db:run(
      #{mnesia => fun() -> get_all_in_mnesia() end
       }).

get_all_in_mnesia() ->
    list_with_possible_retry_in_mnesia(
      fun() ->
              rabbit_db:list_in_mnesia(?MNESIA_TABLE, amqqueue:pattern_match_all())
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
    rabbit_db:run(
      #{mnesia => fun() -> get_all_in_mnesia(VHostName) end
       }).

get_all_in_mnesia(VHostName) ->
    list_with_possible_retry_in_mnesia(
      fun() ->
              Pattern = amqqueue:pattern_match_on_name(rabbit_misc:r(VHostName, queue)),
              rabbit_db:list_in_mnesia(?MNESIA_TABLE, Pattern)
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
    rabbit_db:run(
      #{mnesia => fun() -> get_all_durable_in_mnesia() end
       }).

get_all_durable_in_mnesia() ->
    list_with_possible_retry_in_mnesia(
      fun() ->
              rabbit_db:list_in_mnesia(?MNESIA_DURABLE_TABLE, amqqueue:pattern_match_all())
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
    rabbit_db:run(
      #{mnesia => fun() -> get_all_durable_by_type_in_mnesia(Type) end
       }).

get_all_durable_by_type_in_mnesia(Type) ->
    Pattern = amqqueue:pattern_match_on_type(Type),
    rabbit_db:list_in_mnesia(?MNESIA_DURABLE_TABLE, Pattern).

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
    rabbit_db:run(
      #{mnesia => fun() -> filter_all_durable_in_mnesia(FilterFun) end
       }).

filter_all_durable_in_mnesia(FilterFun) ->
    rabbit_mnesia:execute_mnesia_transaction(
      fun () ->
              qlc:e(qlc:q([Q || Q <- mnesia:table(?MNESIA_DURABLE_TABLE),
                                FilterFun(Q)
                          ]))
      end).

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
    rabbit_db:run(
      #{mnesia => fun() -> list_in_mnesia() end
       }).

list_in_mnesia() ->
    mnesia:dirty_all_keys(?MNESIA_TABLE).

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
    rabbit_db:run(
      #{mnesia => fun() -> count_in_mnesia() end
       }).

count_in_mnesia() ->
    mnesia:table_info(?MNESIA_TABLE, size).

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
            rabbit_log:error("Failed to fetch number of queues in vhost ~p:~n~p",
                             [VHostName, Err]),
            0
    end.

list_for_count(VHostName) ->
    rabbit_db:run(
      #{mnesia => fun() -> list_for_count_in_mnesia(VHostName) end
       }).

list_for_count_in_mnesia(VHostName) ->
    %% this is certainly suboptimal but there is no way to count
    %% things using a secondary index in Mnesia. Our counter-table-per-node
    %% won't work here because with master migration of mirrored queues
    %% the "ownership" of queues by nodes becomes a non-trivial problem
    %% that requires a proper consensus algorithm.
    list_with_possible_retry_in_mnesia(
      fun() ->
              length(mnesia:dirty_index_read(?MNESIA_TABLE,
                                             VHostName,
                                             amqqueue:field_vhost()))
      end).

%% -------------------------------------------------------------------
%% delete().
%% -------------------------------------------------------------------

-spec delete(QName, Reason) -> Ret when
      QName :: rabbit_amqqueue:name(),
      Reason :: atom(),
      Ret :: ok | Deletions :: rabbit_binding:deletions().

delete(QueueName, Reason) ->
    rabbit_db:run(
      #{mnesia => fun() -> delete_in_mnesia(QueueName, Reason) end
       }).

delete_in_mnesia(QueueName, Reason) ->
    rabbit_mnesia:execute_mnesia_transaction(
      fun () ->
              case {mnesia:wread({?MNESIA_TABLE, QueueName}),
                    mnesia:wread({?MNESIA_DURABLE_TABLE, QueueName})} of
                  {[], []} ->
                      ok;
                  _ ->
                      OnlyDurable = case Reason of
                                        missing_owner -> true;
                                        _ -> false
                                    end,
                      internal_delete_in_mnesia(QueueName, OnlyDurable, Reason)
              end
      end).

%% -------------------------------------------------------------------
%% internal_delete().
%% -------------------------------------------------------------------

-spec internal_delete(QName, OnlyDurable, Reason) -> Deletions when
      QName :: rabbit_amqqueue:name(),
      OnlyDurable :: boolean(),
      Reason :: atom(),
      Deletions :: rabbit_binding:deletions().

internal_delete(QueueName, OnlyDurable, Reason) ->
    %% Only used by rabbit_amqqueue:forget_node_for_queue, which is only called
    %% by `rabbit_mnesia:remove_node_if_mnesia_running'. Thus, once mnesia and/or
    %% HA queues are removed it can be removed.
    rabbit_db:run(
      #{mnesia => fun() -> internal_delete_in_mnesia(QueueName, OnlyDurable, Reason) end
       }).

internal_delete_in_mnesia(QueueName, OnlyDurable, Reason) ->
    ok = mnesia:delete({?MNESIA_TABLE, QueueName}),
    case Reason of
        auto_delete ->
            %% efficiency improvement when a channel with many auto-delete queues
            %% is being closed
            case mnesia:wread({?MNESIA_DURABLE_TABLE, QueueName}) of
                []  -> ok;
                [_] -> ok = mnesia:delete({?MNESIA_DURABLE_TABLE, QueueName})
            end;
        _ ->
            mnesia:delete({?MNESIA_DURABLE_TABLE, QueueName})
    end,
    %% we want to execute some things, as decided by rabbit_exchange,
    %% after the transaction.
    rabbit_db_binding:delete_for_destination_in_mnesia(QueueName, OnlyDurable).

%% -------------------------------------------------------------------
%% get_many().
%% -------------------------------------------------------------------

-spec get_many([QName]) -> Ret when
      QName :: rabbit_amqqueue:name(),
      Ret :: [Queue :: amqqueue:amqqueue()].
get_many(Names) when is_list(Names) ->
    rabbit_db:run(
      #{mnesia => fun() -> get_many_in_mnesia(?MNESIA_TABLE, Names) end
       }).

get_many_in_mnesia(Table, [Name]) ->
    ets:lookup(Table, Name);
get_many_in_mnesia(Table, Names) when is_list(Names) ->
    %% Normally we'd call mnesia:dirty_read/1 here, but that is quite
    %% expensive for reasons explained in rabbit_mnesia:dirty_read/1.
    lists:append([ets:lookup(Table, Name) || Name <- Names]).

%% -------------------------------------------------------------------
%% get().
%% -------------------------------------------------------------------

-spec get(QName) -> Ret when
      QName :: rabbit_amqqueue:name(),
      Ret :: {ok, Queue :: amqqueue:amqqueue()} | {error, not_found}.
get(Name) ->
    rabbit_db:run(
      #{mnesia => fun() -> get_in_mnesia(Name) end
       }).

get_in_mnesia(Name) ->
    rabbit_mnesia:dirty_read({?MNESIA_TABLE, Name}).

%% -------------------------------------------------------------------
%% get_durable().
%% -------------------------------------------------------------------

-spec get_durable(QName) -> Ret when
      QName :: rabbit_amqqueue:name(),
      Ret :: {ok, Queue :: amqqueue:amqqueue()} | {error, not_found}.

get_durable(Name) ->
    rabbit_db:run(
      #{mnesia => fun() -> get_durable_in_mnesia(Name) end
       }).

get_durable_in_mnesia(Name) ->
    rabbit_mnesia:dirty_read({?MNESIA_DURABLE_TABLE, Name}).

%% -------------------------------------------------------------------
%% get_many_durable().
%% -------------------------------------------------------------------

-spec get_many_durable([QName]) -> Ret when
      QName :: rabbit_amqqueue:name(),
      Ret :: [Queue :: amqqueue:amqqueue()].

get_many_durable(Names) when is_list(Names) ->
    rabbit_db:run(
      #{mnesia => fun() -> get_many_in_mnesia(?MNESIA_DURABLE_TABLE, Names) end
       }).

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
    rabbit_db:run(
      #{mnesia => fun() -> update_in_mnesia(QName, Fun) end
       }).

update_in_mnesia(QName, Fun) ->
    rabbit_mnesia:execute_mnesia_transaction(
      fun() ->
              update_in_mnesia_tx(QName, Fun)
      end).

%% -------------------------------------------------------------------
%% update_decorators().
%% -------------------------------------------------------------------

-spec update_decorators(QName) -> ok when
      QName :: rabbit_amqqueue:name().
%% @doc Updates an existing queue record adding the active queue decorators.
%%
%% @private

update_decorators(QName) ->
    rabbit_db:run(
      #{mnesia => fun() -> update_decorators_in_mnesia(QName) end
       }).

update_decorators_in_mnesia(Name) ->
    rabbit_mnesia:execute_mnesia_transaction(
      fun() ->
              case mnesia:wread({?MNESIA_TABLE, Name}) of
                  [Q] -> ok = mnesia:write(?MNESIA_TABLE, rabbit_queue_decorator:set(Q),
                                           write);
                  []  -> ok
              end
      end).

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
    rabbit_db:run(
      #{mnesia =>
            fun() -> update_durable_in_mnesia(UpdateFun, FilterFun) end
       }).

update_durable_in_mnesia(UpdateFun, FilterFun) ->
    Pattern = amqqueue:pattern_match_all(),
    {atomic, ok} =
        mnesia:sync_transaction(
          fun () ->
                  Qs = mnesia:match_object(?MNESIA_DURABLE_TABLE, Pattern, write),
                  _ = [mnesia:write(?MNESIA_DURABLE_TABLE, UpdateFun(Q), write)
                       || Q <- Qs, FilterFun(Q)],
                  ok
          end),
    ok.

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

exists(QName) ->
    rabbit_db:run(
      #{mnesia => fun() -> exists_in_mnesia(QName) end
       }).

exists_in_mnesia(QName) ->
    ets:member(?MNESIA_TABLE, QName).

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
    rabbit_db:run(
      #{mnesia => fun() -> consistent_exists_in_mnesia(QName) end
       }).

consistent_exists_in_mnesia(QName) ->
    case mnesia:read({?MNESIA_TABLE, QName}) of
        [] -> false;
        [_] -> true
    end.

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
    rabbit_db:run(
      #{mnesia => fun() -> get_all_by_pattern_in_mnesia(Pattern) end
       }).

get_all_by_pattern_in_mnesia(Pattern) ->
    rabbit_db:list_in_mnesia(?MNESIA_TABLE, Pattern).

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
    rabbit_db:run(
      #{mnesia => fun() -> get_all_by_type_and_node_in_mnesia(VHostName, Type, Node) end
       }).

get_all_by_type_and_node_in_mnesia(VHostName, Type, Node) ->
    mnesia:async_dirty(
      fun () ->
              qlc:e(qlc:q([Q || Q <- mnesia:table(?MNESIA_DURABLE_TABLE),
                                amqqueue:get_type(Q) =:= Type,
                                amqqueue:get_vhost(Q) =:= VHostName,
                                amqqueue:qnode(Q) == Node]))
      end).

%% -------------------------------------------------------------------
%% create_or_get().
%% -------------------------------------------------------------------

-spec create_or_get(Queue) -> Ret when
      Queue :: amqqueue:amqqueue(),
      Ret :: {created, Queue} | {existing, Queue} | {absent, Queue, nodedown}.
%% @doc Writes a queue record if it doesn't exist already or returns the existing one
%%
%% @returns the existing record if there is one in the database already, or the newly
%% created record. 
%%
%% @private

create_or_get(Q) ->
    rabbit_db:run(
      #{mnesia => fun() -> create_or_get_in_mnesia(Q) end
       }).

create_or_get_in_mnesia(Q) ->
    DurableQ = amqqueue:reset_mirroring_and_decorators(Q),
    QueueName = amqqueue:get_name(Q),
    rabbit_mnesia:execute_mnesia_transaction(
      fun () ->
              case mnesia:wread({?MNESIA_TABLE, QueueName}) of
                  [] ->
                      case get_durable_in_mnesia_tx(QueueName) of
                          {error, not_found} ->
                              set_in_mnesia_tx(DurableQ, Q),
                              {created, Q};
                          {ok, Q} ->
                              {absent, Q, nodedown}
                      end;
                  [ExistingQ] ->
                      {existing, ExistingQ}
              end
      end).

%% -------------------------------------------------------------------
%% set().
%% -------------------------------------------------------------------

-spec set(Queue) -> ok when
      Queue :: amqqueue:amqqueue().
%% @doc Writes a queue record. If the queue is durable, it writes both instances:
%% durable and transient. For the durable one, it resets mirrors and decorators.
%% The transient one is left as it is.
%%
%% @private

set(Q) ->
    rabbit_db:run(
      #{mnesia => fun() -> set_in_mnesia(Q) end
       }).

set_in_mnesia(Q) ->
    DurableQ = amqqueue:reset_mirroring_and_decorators(Q),
    rabbit_mnesia:execute_mnesia_transaction(
      fun () ->
              set_in_mnesia_tx(DurableQ, Q)
      end).

set_in_mnesia_tx(DurableQ, Q) ->
    case ?amqqueue_is_durable(Q) of
        true ->
            ok = mnesia:write(?MNESIA_DURABLE_TABLE, DurableQ, write);
        false ->
            ok
    end,
    ok = mnesia:write(?MNESIA_TABLE, Q, write).

%% -------------------------------------------------------------------
%% set_many().
%% -------------------------------------------------------------------

-spec set_many([Queue]) -> ok when
      Queue :: amqqueue:amqqueue().
%% @doc Writes a list of durable queue records.
%% It is responsibility of the calling function to ensure all records are durable.
%% Once transient entities are deprecated, this is a non-issue.
%%
%% @private

set_many(Qs) ->
    rabbit_db:run(
      #{mnesia => fun() -> set_many_in_mnesia(Qs) end
       }).

set_many_in_mnesia(Qs) ->
    {atomic, ok} =
        %% Just to be nested in forget_node_for_queue
        mnesia:transaction(
          fun() ->
                  [ok = mnesia:write(?MNESIA_DURABLE_TABLE, Q, write) || Q <- Qs],
                  ok
          end),
    ok.

%% -------------------------------------------------------------------
%% delete_transient().
%% -------------------------------------------------------------------

-spec delete_transient(FilterFun) -> Ret when
      Queue :: amqqueue:amqqueue(),
      FilterFun :: fun((Queue) -> boolean()),
      QName :: rabbit_amqqueue:name(),
      Ret :: {[QName], [Deletions :: rabbit_binding:deletions()]}.
%% @doc Deletes all transient queues that match `FilterFun'.
%%
%% @private

delete_transient(FilterFun) ->
    rabbit_db:run(
      #{mnesia => fun() -> delete_transient_in_mnesia(FilterFun) end
       }).

delete_transient_in_mnesia(FilterFun) ->
    Qs = rabbit_mnesia:execute_mnesia_transaction(
           fun () ->
                   qlc:e(qlc:q([amqqueue:get_name(Q) || Q <- mnesia:table(?MNESIA_TABLE),
                                                        FilterFun(Q)
                               ]))
           end),
    lists:unzip(lists:flatten(
                  [delete_many_transient_in_mnesia(Queues) || Queues <- partition_queues(Qs)]
                 )).

-spec delete_many_transient_in_mnesia([QName]) -> Ret when
      QName :: rabbit_amqqueue:name(),
      Ret :: [{QName, Deletions :: rabbit_binding:deletions()}].

delete_many_transient_in_mnesia(Queues) ->
    rabbit_mnesia:execute_mnesia_transaction(
      fun () ->
              [{QName, delete_transient_in_mnesia_tx(QName)}
               || QName <- Queues]
      end).

delete_transient_in_mnesia_tx(QName) ->
    ok = mnesia:delete({?MNESIA_TABLE, QName}),
    rabbit_db_binding:delete_transient_for_destination_in_mnesia(QName).

% If there are many queues and we delete them all in a single Mnesia transaction,
% this can block all other Mnesia operations for a really long time.
% In situations where a node wants to (re-)join a cluster,
% Mnesia won't be able to sync on the new node until this operation finishes.
% As a result, we want to have multiple Mnesia transactions so that other
% operations can make progress in between these queue delete transactions.
%
% 10 queues per Mnesia transaction is an arbitrary number, but it seems to work OK with 50k queues per node.
partition_queues([Q0,Q1,Q2,Q3,Q4,Q5,Q6,Q7,Q8,Q9 | T]) ->
    [[Q0,Q1,Q2,Q3,Q4,Q5,Q6,Q7,Q8,Q9] | partition_queues(T)];
partition_queues(T) ->
    [T].

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
    rabbit_db:run(
      #{mnesia => fun() -> foreach_transient_in_mnesia(UpdateFun) end
       }).

foreach_transient_in_mnesia(UpdateFun) ->
    Pattern = amqqueue:pattern_match_all(),
    rabbit_mnesia:execute_mnesia_transaction(
      fun () ->
              Qs = mnesia:match_object(?MNESIA_TABLE, Pattern, write),
              _ = [UpdateFun(Q) || Q <- Qs],
              ok
      end).

%% -------------------------------------------------------------------
%% foreach_durable().
%% -------------------------------------------------------------------

-spec foreach_durable(UpdateFun, FilterFun) -> ok when
      UpdateFun :: fun((Queue) -> any()),
      FilterFun :: fun((Queue) -> boolean()).
%% @doc Applies `UpdateFun' to all durable queue records that match `FilterFun'.
%%
%% @private

foreach_durable(UpdateFun, FilterFun) ->
    rabbit_db:run(
      #{mnesia =>
            fun() -> foreach_durable_in_mnesia(UpdateFun, FilterFun) end
       }).

foreach_durable_in_mnesia(UpdateFun, FilterFun) ->
    %% Note rabbit is not running so we avoid e.g. the worker pool. Also why
    %% we don't invoke the return from rabbit_binding:process_deletions/1.
    Pattern = amqqueue:pattern_match_all(),
    {atomic, ok} =
        mnesia:sync_transaction(
          fun () ->
                  Qs = mnesia:match_object(?MNESIA_DURABLE_TABLE, Pattern, write),
                  _ = [UpdateFun(Q) || Q <- Qs, FilterFun(Q)],
                  ok
          end),
    ok.    

%% -------------------------------------------------------------------
%% set_dirty().
%% -------------------------------------------------------------------

-spec set_dirty(Queue) -> ok when
      Queue :: amqqueue:amqqueue().
%% @doc Writes a transient queue record
%%
%% @private

set_dirty(Q) ->
    rabbit_db:run(
      #{mnesia => fun() -> set_dirty_in_mnesia(Q) end
       }).

set_dirty_in_mnesia(Q) ->
    ok = mnesia:dirty_write(?MNESIA_TABLE, rabbit_queue_decorator:set(Q)).

%% -------------------------------------------------------------------
%% update_in_mnesia_tx().
%% -------------------------------------------------------------------

-spec update_in_mnesia_tx(QName, UpdateFun) -> Ret when
      QName :: rabbit_amqqueue:name(),
      Queue :: amqqueue:amqqueue(),
      UpdateFun :: fun((Queue) -> Queue),
      Ret :: Queue | not_found.

update_in_mnesia_tx(Name, Fun) ->
    case mnesia:wread({?MNESIA_TABLE, Name}) of
        [Q] ->
            Durable = amqqueue:is_durable(Q),
            Q1 = Fun(Q),
            ok = mnesia:write(?MNESIA_TABLE, Q1, write),
            case Durable of
                true -> ok = mnesia:write(?MNESIA_DURABLE_TABLE, Q1, write);
                _    -> ok
            end,
            Q1;
        [] ->
            not_found
    end.

%% -------------------------------------------------------------------
%% get_durable_in_mnesia_tx().
%% -------------------------------------------------------------------

-spec get_durable_in_mnesia_tx(QName) -> Ret when
      QName :: rabbit_amqqueue:name(),
      Ret :: {ok, Queue :: amqqueue:amqqueue()} | {error, not_found}.

get_durable_in_mnesia_tx(Name) ->
    case mnesia:read({?MNESIA_DURABLE_TABLE, Name}) of
        [] -> {error, not_found};
        [Q] -> {ok, Q}
    end.

%% -------------------------------------------------------------------
%% clear().
%% -------------------------------------------------------------------

-spec clear() -> ok.
%% @doc Deletes all queues.
%%
%% @private

clear() ->
    rabbit_db:run(
      #{mnesia => fun() -> clear_in_mnesia() end}).

clear_in_mnesia() ->
    {atomic, ok} = mnesia:clear_table(?MNESIA_TABLE),
    {atomic, ok} = mnesia:clear_table(?MNESIA_DURABLE_TABLE),
    ok.

%% Internal
%% --------------------------------------------------------------

list_with_possible_retry_in_mnesia(Fun) ->
    %% amqqueue migration:
    %% The `rabbit_queue' or `rabbit_durable_queue' tables
    %% might be migrated between the time we query the pattern
    %% (with the `amqqueue' module) and the time we call
    %% `mnesia:dirty_match_object()'. This would lead to an empty list
    %% (no object matching the now incorrect pattern), not a Mnesia
    %% error.
    %%
    %% So if the result is an empty list and the version of the
    %% `amqqueue' record changed in between, we retry the operation.
    %%
    %% However, we don't do this if inside a Mnesia transaction: we
    %% could end up with a live lock between this started transaction
    %% and the Mnesia table migration which is blocked (but the
    %% rabbit_feature_flags lock is held).
    AmqqueueRecordVersion = amqqueue:record_version_to_use(),
    case Fun() of
        [] ->
            case mnesia:is_transaction() of
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
