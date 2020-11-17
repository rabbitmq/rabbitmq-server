%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2020 VMware, Inc. or its affiliates.  All rights reserved.
%%

% The rabbit_transform interface creates a rabbit_transform Mnesia table for
% retaining transformation identities and versions that have been applied to the
% broker. This interface can be used for any other internal entity that undergoes
% a change/transformation - whose name and version needs to retained be in the
% broker. The actual transform function remains private / hard-codedin the system.
% During upgrade, permission to carry-out certains "steps" is queried through a
% rabbit_transform:is_permited/{1,2} operation, which checks for any active
% partitions or alarms.

-module(rabbit_transform).

-export([table_name/0, create_table/0]).
-export([new/2, new/3, add/1, add/3, lookup/1, exists/2]).
-export([check_safety/1, is_permitted/0, is_permitted/1]).
-export([delete/1, delete_version/2]).
-export([format/1, log_info/2, log_success/2, log_success/3, log_error/3]).
-export([get_timeout/0, get_queue_index_transform_timeout/0]).
-export([add_delivery_count_to_classic_queue_index/1]).

-include("rabbit.hrl").

-define(TAB, ?MODULE).

%% Transform timeouts
-define(TRANSFORM_TIMEOUT, application:get_env(rabbit, transform_timeout, 300000)).
-define(QUEUE_INDEX_TRANSFORM_TIMEOUT,
    application:get_env(rabbit, queue_index_transform_timeout, 600000)).

%% Queue transform thresholds
-define(QUEUE_TRANSFORM_MESSAGE_THRESHOLD,
    application:get_env(rabbit, queue_transform_message_threshold, 100000)).
-define(QUEUE_TRANSFORM_MEMORY_THRESHOLD,
    application:get_env(rabbit, queue_transform_memory_threshold, 1073741824)).
-define(QUEUE_TRANSFORM_TOTAL_MESSAGE_THRESHOLD,
    application:get_env(rabbit, queue_transform_total_message_threshold, 10000000)).

-spec table_name() -> atom().
table_name() -> ?TAB.

-spec create_table() -> rabbit_types:ok_or_error(any()).
create_table() ->
    try
        rabbit_table:create(table_name(),
            [{record_name, transform},
             {attributes, record_info(fields, transform)}])
    catch
        throw:Reason -> {error, Reason}
    end.

-spec new(rabbit_types:transform_name(), rabbit_types:transform_version()) ->
    rabbit_types:transform().
new(Name, Version) ->
    new(Name, Version, #{}).

-spec new(rabbit_types:transform_name(),
          rabbit_types:transform_version() | [rabbit_types:transform_version()],
          rabbit_types:transform_options()) -> rabbit_types:transform().
new(Name, Versions, Options) when is_list(Versions) ->
    #transform{
        name     = Name,
        versions = Versions,
        options  = Options
    };
new(Name, Version, Options) ->
    new(Name, [Version], Options).

-spec add(rabbit_types:transform()) -> ok.
add(#transform{name     = Name,
               versions = Versions,
               options  = Options
              }) ->
    add(Name, Versions, Options).

-spec add(rabbit_types:transform_name(),
          rabbit_types:transform_version() | [rabbit_types:transform_version()],
          rabbit_types:transform_options()) -> ok.
add(Name, Versions, Options) when is_list(Versions), is_map(Options) ->
    rabbit_misc:execute_mnesia_transaction(
        fun() ->
            case mnesia:wread({?TAB, Name}) of
              [#transform{versions = Versions1, options = Options1} = TF] ->
                  TF1 = TF#transform{versions = lists:usort(lists:append(Versions, Versions1)),
                                     options  = maps:merge(Options, Options1)},
                  ok = mnesia:write(?TAB, TF1, write);
              [] ->
                  TF = new(Name, Versions, Options),
                  ok = mnesia:write(?TAB, TF, write)
            end
        end);
add(Name, Version, Options) when is_map(Options) ->
    add(Name, [Version], Options).

-spec lookup(rabbit_types:transform_name()) ->
    {ok, rabbit_types:transform()} | rabbit_types:error(any()).
lookup(Name) ->
    rabbit_misc:dirty_read({?TAB, Name}).

-spec exists(rabbit_types:transform_name(), rabbit_types:transform_version()) -> boolean().
exists(Name, Version) ->
    try lookup(Name) of
        {ok, #transform{versions = Versions}} ->
            lists:member(Version, Versions);
        _ ->
            false
    catch
        _:_ -> false
    end.

-spec check_safety([amqqueue:amqqueue()]) -> rabbit_types:ok_or_error(any()).
check_safety(Queues) ->
    %% Ensure permitted (no alarms and partition state)
    case is_permitted() of
        true  -> ok;
        false -> throw({error, transform_not_permitted})
    end,

    %% Check transform thresholds aren't exceeded
    TotalMsgs =
        lists:sum(
            [begin
                QInfo = rabbit_amqqueue:info(Q, [messages, memory]),
                NMessages = rabbit_misc:pget(messages, QInfo),
                Memory = rabbit_misc:pget(memory, QInfo),
                QName = rabbit_misc:rs(amqqueue:get_name(Q)),
                check_safety(QName, NMessages, ?QUEUE_TRANSFORM_MESSAGE_THRESHOLD,
                    transform_message_threshold_exceeded),
                check_safety(QName, Memory, ?QUEUE_TRANSFORM_MEMORY_THRESHOLD,
                    transform_memory_threshold_exceeded),
                NMessages
             end || Q <- Queues]),

    %% Check transform total message threshold isn't exceeded
    case TotalMsgs =< ?QUEUE_TRANSFORM_TOTAL_MESSAGE_THRESHOLD of
        true ->
            ok;
        false ->
            throw(transform_total_message_threshold_exceeded)
    end.

-spec is_permitted() -> boolean().
is_permitted() ->
    (rabbit_node_monitor:pause_partition_guard() =:= ok) and
        (rabbit_alarm:lookup_alarms() =:= []).

-spec is_permitted([pid() | node()]) -> boolean().
is_permitted(NodesOrPids) ->
    Result =
        (is_permitted()) and
            (lists:all(fun(NodeOrPid) -> check_if_permitted(NodeOrPid) end, NodesOrPids)),
    timer:sleep(100),
    Result.

-spec delete(rabbit_types:transform_name()) -> ok.
delete(Name) ->
    rabbit_misc:execute_mnesia_transaction(
        fun() -> mnesia:delete({?TAB, Name}) end).

-spec delete_version(rabbit_types:transform_name(), rabbit_types:transform_version()) -> ok.
delete_version(Name, Version) ->
    rabbit_misc:execute_mnesia_transaction(
        fun() ->
            try lookup(Name) of
                {ok, TF = #transform{versions = Versions}} ->
                    case lists:member(Version, Versions) of
                        true ->
                            Versions1 = lists:delete(Version, Versions),
                            TF1 = TF#transform{versions = Versions1},
                            ok = mnesia:write(?TAB, TF1, write);
                        false ->
                            ok
                    end;
                _ ->
                    ok
            catch
                _:_ -> ok
            end
        end).

-spec get_timeout() -> non_neg_integer().
get_timeout() -> ?TRANSFORM_TIMEOUT.

-spec get_queue_index_transform_timeout() -> non_neg_integer().
get_queue_index_transform_timeout() -> ?QUEUE_INDEX_TRANSFORM_TIMEOUT.

-spec format(rabbit_types:transform()) -> string().
format(#transform{name = Name, versions = [Version]}) ->
    rabbit_misc:format("transform ~p of version ~p", [Name, Version]).

-spec log_info(any(), rabbit_types:transform()) -> ok.
log_info(Element, TF = #transform{}) ->
    rabbit_log:info("Transforming ~s using ~s", [Element, format(TF)]).

-spec log_success(any(), rabbit_types:transform()) -> ok.
log_success(Element, TF = #transform{}) ->
    rabbit_log:info("Successfully transformed ~s using ~s", [Element, format(TF)]).

-spec log_success(any(), rabbit_types:transform(), pos_integer()) -> ok.
log_success(Element, TF = #transform{}, Time) ->
    rabbit_log:info("Successfully transformed ~s using ~s in ~p µs",
        [Element, format(TF), Time]).

-spec log_error(any(), rabbit_types:transform(), any()) -> ok.
log_error(Element, TF = #transform{}, Reason) ->
    Err = rabbit_misc:format("Failed transformation of ~s using ~s with reason: ~w",
              [Element, format(TF), Reason]),
    rabbit_log:error(Err).

-spec add_delivery_count_to_classic_queue_index([node()]) -> rabbit_types:ok_or_error(any()).
add_delivery_count_to_classic_queue_index(Nodes) ->
    case rabbit_transform:is_permitted(Nodes) of
        true ->
            Results =
                [rpc:call(N, rabbit_queue_index, add_delivery_count, [],
                    rabbit_transform:get_queue_index_transform_timeout())
                  || N <- Nodes],
            case lists:delete(ok, Results) of
                []     -> ok;
                Errors -> Errors
            end;
        false ->
            {error, transform_queue_index_not_permitted}
    end.

%% Internal
check_safety(QName, Messages, Threshold, Error) ->
    case (Messages =< Threshold) of
        true  -> ok;
        false -> throw({QName, Error})
    end.

check_if_permitted(Pid) when is_pid(Pid) ->
    rabbit_misc:is_process_alive(Pid);
check_if_permitted(Node) when is_atom(Node) ->
    net_adm:ping(Node) =:= pong.
