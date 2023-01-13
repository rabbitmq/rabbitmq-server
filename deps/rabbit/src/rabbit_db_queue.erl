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
         get_all/0,
         get_all/1,
         get_all_by_type/1,
         get_all_by_type_and_node/3,
         list/0,
         count/0,
         count/1,
         create_or_get/2,
         insert/2,
         insert/1,
         delete/2,
         update/2,
         update_decorators/1,
         exists/1
        ]).

-export([
         get_all_durable/0,
         get_all_durable/1,
         get_all_durable_by_type/1,
         get_durable/1
        ]).

-export([delete_transient/1]).
-export([on_node_up/2,
         on_node_down/2]).

-export([match_and_update/3]).
-export([insert_dirty/1]).

-export([not_found_or_absent_queue_dirty/1]).

-export([internal_delete/3]).

%% Used by other rabbit_db_* modules
-export([
         update_in_mnesia_tx/2,
         not_found_or_absent_queue_in_mnesia/1
        ]).

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
              rabbit_db:list_in_mnesia(rabbit_queue, amqqueue:pattern_match_all())
      end).

-spec get_all(VHostName) -> [Queue] when
      VHostName :: vhost:name(),
      Queue :: amqqueue:amqqueue().

%% @doc Gets all queues belonging to the given virtual host
%%
%% @returns a list of queue records.
%%
%% @private

get_all(VHost) ->
    rabbit_db:run(
      #{mnesia => fun() -> get_all_in_mnesia(VHost) end
       }).

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
              rabbit_db:list_in_mnesia(rabbit_durable_queue, amqqueue:pattern_match_all())
      end).

-spec get_all_durable(VHostName) -> [Queue] when
      VHostName :: vhost:name(),
      Queue :: amqqueue:amqqueue().

%% @doc Gets all durable queues belonging to the given virtual host
%%
%% @returns a list of queue records.
%%
%% @private

get_all_durable(VHost) ->
    rabbit_db:run(
      #{mnesia => fun() -> get_all_durable_in_mnesia(VHost) end
       }).

get_all_durable_in_mnesia(VHost) ->
    list_with_possible_retry_in_mnesia(
      fun() ->
              Pattern = amqqueue:pattern_match_on_name(rabbit_misc:r(VHost, queue)),
              rabbit_db:list_in_mnesia(rabbit_durable_queue, Pattern)
      end).

get_all_durable_by_type(Type) ->
    rabbit_db:run(
      #{mnesia => fun() -> get_all_durable_by_type_in_mnesia(Type) end
       }).

get_all_durable_by_type_in_mnesia(Type) ->
    Pattern = amqqueue:pattern_match_on_type(Type),
    rabbit_db:list_in_mnesia(rabbit_durable_queue, Pattern).

list() ->
    rabbit_db:run(
      #{mnesia => fun() -> list_in_mnesia() end
       }).

list_in_mnesia() ->
    mnesia:dirty_all_keys(rabbit_queue).

count() ->
    rabbit_db:run(
      #{mnesia => fun() -> count_in_mnesia() end
       }).

count_in_mnesia() ->
    mnesia:table_info(rabbit_queue, size).

count(VHost) ->
    try
        list_for_count(VHost)
    catch _:Err ->
            rabbit_log:error("Failed to fetch number of queues in vhost ~p:~n~p",
                             [VHost, Err]),
            0
    end.

delete(QueueName, Reason) ->
    rabbit_db:run(
      #{mnesia => fun() -> delete_in_mnesia(QueueName, Reason) end
       }).

internal_delete(QueueName, OnlyDurable, Reason) ->
    %% Only used by rabbit_amqqueue:forget_node_for_queue, which is only called
    %% by `rabbit_mnesia:remove_node_if_mnesia_running`. Thus, once mnesia and/or
    %% HA queues are removed it can be removed.
    rabbit_db:run(
      #{mnesia => fun() -> internal_delete_in_mnesia(QueueName, OnlyDurable, Reason) end
       }).

get(Names) when is_list(Names) ->
    rabbit_db:run(
      #{mnesia => fun() -> get_many_in_mnesia(rabbit_queue, Names) end
       });
get(Name) ->
    rabbit_db:run(
      #{mnesia => fun() -> get_in_mnesia(Name) end
       }).

get_in_mnesia(Name) ->
    rabbit_mnesia:dirty_read({rabbit_queue, Name}).

get_durable(Names) when is_list(Names) ->
    rabbit_db:run(
      #{mnesia => fun() -> get_many_in_mnesia(rabbit_durable_queue, Names) end
       });
get_durable(Name) ->
    rabbit_db:run(
      #{mnesia => fun() -> get_durable_in_mnesia(Name) end
       }).

get_durable_in_mnesia(Name) ->
    rabbit_mnesia:dirty_read({rabbit_durable_queue, Name}).

delete_transient(Queues) ->
    rabbit_db:run(
      #{mnesia => fun() -> delete_transient_in_mnesia(Queues) end
       }).

delete_transient_in_mnesia(Queues) ->
    rabbit_mnesia:execute_mnesia_transaction(
      fun () ->
              [{QName, delete_transient_in_mnesia_tx(QName)}
               || QName <- Queues]
      end).

on_node_up(Node, Fun) ->
    rabbit_db:run(
      #{mnesia => fun() -> on_node_up_in_mnesia(Node, Fun) end
       }).

on_node_up_in_mnesia(Node, Fun) ->
    rabbit_mnesia:execute_mnesia_transaction(
      fun () ->
              Qs = mnesia:match_object(rabbit_queue,
                                       amqqueue:pattern_match_all(), write),
              [Fun(Node, Q) || Q <- Qs],
              ok
      end).

on_node_down(Node, Fun) ->
    rabbit_db:run(
      #{mnesia => fun() -> on_node_down_in_mnesia(Node, Fun) end
       }).

on_node_down_in_mnesia(Node, Fun) ->
    Qs = rabbit_mnesia:execute_mnesia_transaction(
           fun () ->
                   qlc:e(qlc:q([amqqueue:get_name(Q) || Q <- mnesia:table(rabbit_queue),
                                                        Fun(Node, Q)
                               ]))
           end),
    lists:unzip(lists:flatten(
                  [case delete_transient(Queues) of
                       {error, noproc} -> [];
                       {error, {timeout, _}} -> [];
                       Value -> Value
                   end || Queues <- partition_queues(Qs)]
                 )).

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

update(QName, Fun) ->
    rabbit_db:run(
      #{mnesia => fun() -> update_in_mnesia(QName, Fun) end
       }).

update_in_mnesia(QName, Fun) ->
    rabbit_mnesia:execute_mnesia_transaction(
      fun() ->
              update_in_mnesia_tx(QName, Fun)
      end).

update_decorators(Name) ->
    rabbit_db:run(
      #{mnesia => fun() -> update_decorators_in_mnesia(Name) end
       }).

not_found_or_absent_queue_dirty(Name) ->
    rabbit_db:run(
      #{mnesia => fun() -> not_found_or_absent_queue_dirty_in_mnesia(Name) end
       }).

exists(Name) ->
    rabbit_db:run(
      #{mnesia => fun() -> exists_in_mnesia(Name) end
       }).

exists_in_mnesia(Name) ->
    ets:member(rabbit_queue, Name).

get_all_by_type(Type) ->
    Pattern = amqqueue:pattern_match_on_type(Type),
    rabbit_db:run(
      #{mnesia => fun() -> get_all_by_pattern_in_mnesia(Pattern) end
       }).

get_all_by_pattern_in_mnesia(Pattern) ->
    rabbit_db:list_in_mnesia(rabbit_queue, Pattern).

get_all_by_type_and_node(VHost, Type, Node) ->
    rabbit_db:run(
      #{mnesia => fun() -> get_all_by_type_and_node_in_mnesia(VHost, Type, Node) end
       }).

get_all_by_type_and_node_in_mnesia(VHost, Type, Node) ->
    mnesia:async_dirty(
      fun () ->
              qlc:e(qlc:q([Q || Q <- mnesia:table(rabbit_durable_queue),
                                amqqueue:get_type(Q) =:= Type,
                                amqqueue:get_vhost(Q) =:= VHost,
                                amqqueue:qnode(Q) == Node]))
      end).

create_or_get(DurableQ, Q) ->
    rabbit_db:run(
      #{mnesia => fun() -> create_or_get_in_mnesia(DurableQ, Q) end
       }).

create_or_get_in_mnesia(DurableQ, Q) ->
    QueueName = amqqueue:get_name(Q),
    rabbit_mnesia:execute_mnesia_transaction(
      fun () ->
              case mnesia:wread({rabbit_queue, QueueName}) of
                  [] ->
                      case not_found_or_absent_queue_in_mnesia(QueueName) of
                          not_found           ->
                              insert_in_mnesia_tx(DurableQ, Q),
                              {created, Q};
                          {absent, _Q, _} = R ->
                              R
                      end;
                  [ExistingQ] ->
                      {existing, ExistingQ}
              end
      end).

insert(DurableQ, Q) ->
    rabbit_db:run(
      #{mnesia => fun() -> insert_in_mnesia(DurableQ, Q) end
       }).

insert_in_mnesia(DurableQ, Q) ->
    rabbit_mnesia:execute_mnesia_transaction(
      fun () ->
              insert_in_mnesia_tx(DurableQ, Q)
      end).

insert(Qs) ->
    rabbit_db:run(
      #{mnesia => fun() -> insert_many_in_mnesia(Qs) end
       }).

insert_many_in_mnesia(Qs) ->
    rabbit_mnesia:execute_mnesia_transaction(
      fun() ->
              [ok = mnesia:write(rabbit_durable_queue, Q, write) || Q <- Qs]
      end).

match_and_update(Pattern, UpdateFun, FilterFun) ->
    rabbit_db:run(
      #{mnesia =>
            fun() -> match_and_update_in_mnesia(Pattern, UpdateFun, FilterFun) end
       }).

match_and_update_in_mnesia(Pattern, UpdateFun, FilterFun) ->
    %% Note rabbit is not running so we avoid e.g. the worker pool. Also why
    %% we don't invoke the return from rabbit_binding:process_deletions/1.
    {atomic, ok} =
        mnesia:sync_transaction(
          fun () ->
                  Qs = mnesia:match_object(rabbit_durable_queue, Pattern, write),
                  _ = [UpdateFun(Q) || Q <- Qs, FilterFun(Q)],
                  ok
          end),
    ok.    

insert_dirty(Q) ->
    rabbit_db:run(
      #{mnesia => fun() -> insert_dirty_in_mnesia(Q) end
       }).

insert_dirty_in_mnesia(Q) ->
    ok = mnesia:dirty_write(rabbit_queue, rabbit_queue_decorator:set(Q)).

update_in_mnesia_tx(Name, Fun) ->
    case mnesia:wread({rabbit_queue, Name}) of
        [Q] ->
            Durable = amqqueue:is_durable(Q),
            Q1 = Fun(Q),
            ok = mnesia:write(rabbit_queue, Q1, write),
            case Durable of
                true -> ok = mnesia:write(rabbit_durable_queue, Q1, write);
                _    -> ok
            end,
            Q1;
        [] ->
            not_found
    end.

not_found_or_absent_queue_in_mnesia(Name) ->
    %% NB: we assume that the caller has already performed a lookup on
    %% rabbit_queue and not found anything
    case mnesia:read({rabbit_durable_queue, Name}) of
        []  -> not_found;
        [Q] -> {absent, Q, nodedown} %% Q exists on stopped node
    end.

%% Internal
%% --------------------------------------------------------------
get_many_in_mnesia(Table, [Name]) ->
    ets:lookup(Table, Name);
get_many_in_mnesia(Table, Names) when is_list(Names) ->
    %% Normally we'd call mnesia:dirty_read/1 here, but that is quite
    %% expensive for reasons explained in rabbit_mnesia:dirty_read/1.
    lists:append([ets:lookup(Table, Name) || Name <- Names]).

delete_transient_in_mnesia_tx(QName) ->
    ok = mnesia:delete({rabbit_queue, QName}),
    rabbit_db_binding:delete_transient_for_destination_in_mnesia(QName).

get_all_in_mnesia(VHost) ->
    list_with_possible_retry_in_mnesia(
      fun() ->
              Pattern = amqqueue:pattern_match_on_name(rabbit_misc:r(VHost, queue)),
              rabbit_db:list_in_mnesia(rabbit_queue, Pattern)
      end).

not_found_or_absent_queue_dirty_in_mnesia(Name) ->
    %% We should read from both tables inside a tx, to get a
    %% consistent view. But the chances of an inconsistency are small,
    %% and only affect the error kind.
    case rabbit_mnesia:dirty_read({rabbit_durable_queue, Name}) of
        {error, not_found} -> not_found;
        {ok, Q}            -> {absent, Q, nodedown}
    end.

list_with_possible_retry_in_mnesia(Fun) ->
    %% amqqueue migration:
    %% The `rabbit_queue` or `rabbit_durable_queue` tables
    %% might be migrated between the time we query the pattern
    %% (with the `amqqueue` module) and the time we call
    %% `mnesia:dirty_match_object()`. This would lead to an empty list
    %% (no object matching the now incorrect pattern), not a Mnesia
    %% error.
    %%
    %% So if the result is an empty list and the version of the
    %% `amqqueue` record changed in between, we retry the operation.
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

delete_in_mnesia(QueueName, Reason) ->
    rabbit_mnesia:execute_mnesia_transaction(
      fun () ->
              case {mnesia:wread({rabbit_queue, QueueName}),
                    mnesia:wread({rabbit_durable_queue, QueueName})} of
                  {[], []} ->
                      ok;
                  _ ->
                      internal_delete_in_mnesia(QueueName, false, Reason)
              end
      end).

internal_delete_in_mnesia(QueueName, OnlyDurable, Reason) ->
    ok = mnesia:delete({rabbit_queue, QueueName}),
    case Reason of
        auto_delete ->
            case mnesia:wread({rabbit_durable_queue, QueueName}) of
                []  -> ok;
                [_] -> ok = mnesia:delete({rabbit_durable_queue, QueueName})
            end;
        _ ->
            mnesia:delete({rabbit_durable_queue, QueueName})
    end,
    %% we want to execute some things, as decided by rabbit_exchange,
    %% after the transaction.
    rabbit_db_binding:delete_for_destination_in_mnesia(QueueName, OnlyDurable).

list_for_count(VHost) ->
    rabbit_db:run(
      #{mnesia => fun() -> list_for_count_in_mnesia(VHost) end
       }).

list_for_count_in_mnesia(VHost) ->
    %% this is certainly suboptimal but there is no way to count
    %% things using a secondary index in Mnesia. Our counter-table-per-node
    %% won't work here because with master migration of mirrored queues
    %% the "ownership" of queues by nodes becomes a non-trivial problem
    %% that requires a proper consensus algorithm.
    list_with_possible_retry_in_mnesia(
      fun() ->
              length(mnesia:dirty_index_read(rabbit_queue,
                                             VHost,
                                             amqqueue:field_vhost()))
      end).

update_decorators_in_mnesia(Name) ->
    rabbit_mnesia:execute_mnesia_transaction(
      fun() ->
              case mnesia:wread({rabbit_queue, Name}) of
                  [Q] -> ok = mnesia:write(rabbit_queue, rabbit_queue_decorator:set(Q),
                                           write);
                  []  -> ok
              end
      end).

insert_in_mnesia_tx(DurableQ, Q) ->
    case ?amqqueue_is_durable(Q) of
        true ->
            ok = mnesia:write(rabbit_durable_queue, DurableQ, write);
        false ->
            ok
    end,
    ok = mnesia:write(rabbit_queue, Q, write).
