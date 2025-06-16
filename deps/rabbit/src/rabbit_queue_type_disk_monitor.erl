%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_queue_type_disk_monitor).

%% A server for alarming on high disk usage per queue type.
%%
%% The server scans periodically and checks each queue type against its limit
%% using the `disk_footprint/0' and `disk_limit/0' callbacks in
%% `rabbit_queue_type'. Typically this callback uses `rabbit_disk_usage:scan/1'.
%%
%% Also see `rabbit_disk_monitoring' which periodically checks the total space
%% taken on the mounted disk containing `rabbit:data_dir/0'.

-include_lib("kernel/include/logger.hrl").

-behaviour(gen_server).

-export([start_link/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(limit, {queue_type :: queue_type(),
                type_module :: module(),
                limit :: Bytes :: non_neg_integer()}).

-record(state, {limits :: [#limit{}],
                alarmed = alarmed() :: alarmed(),
                timer :: timer:tref() | undefined}).

-type queue_type() :: atom().
-type alarmed() :: sets:set(queue_type()).

-type disk_usage_limit_spec() :: %% A total number of bytes
                                 {absolute, non_neg_integer()} |
                                 %% %% A fraction of the disk's capacity.
                                 %% {relative, float()} |
                                 %% A string which will be parsed and
                                 %% interpreted as an absolute limit.
                                 string().

%%----------------------------------------------------------------------------

start_link() ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init([]) ->
    Limits = lists:foldl(
               fun({Type, TypeModule}, Acc) ->
                       case get_limit(Type, TypeModule) of
                           {ok, Limit} ->
                               [#limit{queue_type = Type,
                                       type_module = TypeModule,
                                       limit = Limit} | Acc];
                           error ->
                               Acc
                       end
               end, [], rabbit_registry:lookup_all(queue)),
    Timer = erlang:send_after(5_000, self(), scan),
    {ok, #state{limits = Limits, timer = Timer}}.

handle_call(_Request, _From, State) ->
    {noreply, State}.

handle_cast(_Request, State) ->
    {noreply, State}.

handle_info(scan, #state{alarmed = Alarmed0} = State) ->
    Alarmed = lists:foldl(fun scan/2, alarmed(), State#state.limits),
    ok = handle_alarmed(Alarmed0, Alarmed),
    Timer = erlang:send_after(5_000, self(), scan),
    {noreply, State#state{alarmed = Alarmed, timer = Timer}};
handle_info(Info, State) ->
    ?LOG_DEBUG("~tp unhandled msg: ~tp", [?MODULE, Info]),
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%----------------------------------------------------------------------------

alarmed() -> sets:new([{version, 2}]).

-spec get_limit(atom(), module()) -> {ok, disk_usage_limit_spec()} | error.
get_limit(QType, QTypeModule) ->
    try QTypeModule:disk_limit() of
        undefined ->
            error;
        {absolute, Abs} when is_integer(Abs) andalso Abs >= 0 ->
            {ok, Abs};
        %% {relative, Rel} when is_float(Rel) andalso Rel >= 0.0 ->
        %%     TODO: to convert to abs we need to cache the disk capacity for
        %%     the first `relative' spec we see.
        %%     Do we even need relative? Should it be proportional to the disk
        %%     capacity or to the other components?
        %%     {ok, {relative, Rel}};
        String when is_list(String) ->
            case rabbit_resource_monitor_misc:parse_information_unit(String) of
                {ok, Bytes} ->
                    {ok, Bytes};
                {error, parse_error} ->
                    ?LOG_WARNING("Unable to parse disk limit ~tp for queue "
                                 "type '~ts'", [String, QType]),
                    error
            end
    catch
        error:undef ->
            error
    end.

-spec scan(Limit :: #limit{}, alarmed()) -> alarmed().
scan(#limit{queue_type = QType,
            type_module = QTypeModule,
            limit = Limit}, Alarmed) ->
    %% NOTE: `disk_footprint/0' is an optional callback but it should always
    %% be implemented if the queue type implements `disk_limit/0'.
    case QTypeModule:disk_footprint() of
        {ok, Bytes} ->
            %% TODO: remove this printf debugging...
            ?LOG_INFO("Measured queue type '~ts' at ~p bytes (limit ~p)", [QType, Bytes, Limit]),
            case Bytes >= Limit of
                true -> sets:add_element(QTypeModule, Alarmed);
                false -> Alarmed
            end;
        {error, enoent} ->
            Alarmed;
        {error, Error} ->
            ?LOG_WARNING("Failed to calculate disk usage of queue type '~ts': "
                         "~tp", [QType, Error]),
            Alarmed
    end.

-spec handle_alarmed(Before :: alarmed(), After :: alarmed()) -> ok.
handle_alarmed(NoChange, NoChange) ->
    ok;
handle_alarmed(Before, After) ->
    Added = sets:subtract(After, Before),
    ?LOG_WARNING("Newly alarmed: ~p", [Added]),
    ok = sets:fold(
           fun(QType, ok) ->
                   rabbit_alarm:set_alarm({alarm(QType), []})
           end, ok, Added),
    Removed = sets:subtract(Before, After),
    ?LOG_WARNING("Stopped alarming: ~p", [Removed]),
    ok = sets:fold(
           fun(QType, ok) ->
                   rabbit_alarm:clear_alarm(alarm(QType))
           end, ok, Removed),
    ok.

alarm(QType) ->
    {resource_limit, {queue_type_disk, QType}, node()}.
