%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%
-module(rabbit_mgmt_sessions).

-behaviour(gen_server).

-export([start_link/0]).
-export([create_session/2, touch/2, delete_session/1, delete_session/2,
         list_sessions/3, terminate_sessions/1]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-include_lib("kernel/include/logger.hrl").

-record(session, {
    created_at :: integer(),
    expires_at :: integer(),
    metadata   :: #{binary() => binary()}
}).

-record(state, {
    timer :: reference() | undefined
}).

-define(SWEEP_INTERVAL, 5000).

-define(KHEPRI_USER_SESSIONS_PATTERN(Username), [rabbitmq, users, Username, sessions, '?']).
-define(KHEPRI_SESSION_PATH(Username, SessionId), [rabbitmq, users, Username, sessions, SessionId]).
-define(KHEPRI_ALL_SESSIONS_PATTERN, [rabbitmq, users, '?', sessions, '?']).
-define(KHEPRI_SESSION_ID_PATTERN(SessionId), [rabbitmq, users, '?', sessions, SessionId]).

%%====================================================================
%% API
%%====================================================================

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

create_session(Username, Metadata) ->
    SessionId = list_to_binary(rabbit_guid:to_string(rabbit_guid:gen())),
    Now = os:system_time(millisecond),
    ExpiresAt = calculate_expires_at(Now),
    Session = #session{
        created_at = Now,
        expires_at = ExpiresAt,
        metadata = Metadata
    },
    Settings = rabbit_mgmt_features:get_sessions_settings(),
    MaxConcurrent = proplists:get_value(max_concurrent, Settings, 1),
    UserSessionsPath = ?KHEPRI_USER_SESSIONS_PATTERN(Username),
    SessionPath = ?KHEPRI_SESSION_PATH(Username, SessionId),

    TxRes = rabbit_khepri:transaction(fun() ->
        Map = case khepri_tx:get_many(UserSessionsPath) of
            {ok, M} -> M;
            _       -> #{}
        end,
        ActiveSessions = maps:fold(fun(_P, S, Acc) ->
            if is_record(S, session) andalso S#session.expires_at > Now ->
                   [S | Acc];
               true ->
                   Acc
            end
        end, [], Map),
        if length(ActiveSessions) >= MaxConcurrent ->
            khepri_tx:abort(limit_reached);
           true ->
            case khepri_tx:put(SessionPath, Session) of
                ok -> SessionId;
                Err -> khepri_tx:abort(Err)
            end
        end
    end),
    case TxRes of
        {ok, SessionId} ->
            ?LOG_DEBUG("Created session ~s for user ~s", [SessionId, Username]),
            {ok, SessionId};
        {error, limit_reached} ->
            ?LOG_DEBUG("Failed to create session for user ~s: concurrent session limit reached", [Username]),
            {error, limit_reached};
        {error, Reason} ->
            {error, Reason}
    end.

touch(undefined, _Username) ->
    {error, not_found};
touch(_SessionId, undefined) ->
    {error, not_found};
touch(SessionId, Username) ->
    Now = os:system_time(millisecond),
    SessionPath = ?KHEPRI_SESSION_PATH(Username, SessionId),

    TxRes = rabbit_khepri:transaction(fun() ->
        case khepri_tx:get(SessionPath) of
            {ok, Session} when is_record(Session, session) ->
                if Session#session.expires_at > Now ->
                    NewExpiresAt = calculate_expires_at(Session#session.created_at, Now),
                    NewSession = Session#session{expires_at = NewExpiresAt},
                    case khepri_tx:put(SessionPath, NewSession) of
                        ok -> ok;
                        Err -> khepri_tx:abort(Err)
                    end;
                   true ->
                    khepri_tx:abort(not_found)
                end;
            _ ->
                PathPattern = ?KHEPRI_SESSION_ID_PATTERN(SessionId),
                case khepri_tx:get_many(PathPattern) of
                    {ok, Map} when map_size(Map) > 0 ->
                        ActiveOther = maps:fold(fun(_P, S, Acc) ->
                            if is_record(S, session) andalso S#session.expires_at > Now ->
                                   true;
                               true ->
                                   Acc
                            end
                        end, false, Map),
                        if ActiveOther ->
                            khepri_tx:abort(forbidden);
                           true ->
                            khepri_tx:abort(not_found)
                        end;
                    _ ->
                        khepri_tx:abort(not_found)
                end
        end
    end),
    case TxRes of
        {ok, ok} -> ok;
        {error, forbidden} -> {error, forbidden};
        {error, not_found} -> {error, not_found};
        {error, Reason} -> {error, Reason}
    end.

delete_session(undefined) ->
    {error, not_found};
delete_session(SessionId) ->
    delete_session(SessionId, undefined).

delete_session(undefined, _Username) ->
    {error, not_found};
delete_session(SessionId, undefined) ->
    PathPattern = ?KHEPRI_SESSION_ID_PATTERN(SessionId),
    case rabbit_khepri:get_many(PathPattern) of
        {ok, Map} when map_size(Map) > 0 ->
            lists:foreach(fun(Path) ->
                _ = rabbit_khepri:delete(Path)
            end, maps:keys(Map)),
            ok;
        _ ->
            {error, not_found}
    end;
delete_session(SessionId, Username) ->
    SessionPath = ?KHEPRI_SESSION_PATH(Username, SessionId),
    case rabbit_khepri:get(SessionPath) of
        {ok, Session} when is_record(Session, session) ->
            _ = rabbit_khepri:delete(SessionPath),
            ok;
        _ ->
            PathPattern = ?KHEPRI_SESSION_ID_PATTERN(SessionId),
            case rabbit_khepri:get_many(PathPattern) of
                {ok, Map} when map_size(Map) > 0 ->
                    {error, forbidden};
                _ ->
                    {error, not_found}
            end
    end.

list_sessions(Page, PageSize, UsernameFilter) ->
    PathPattern = case UsernameFilter of
        undefined -> ?KHEPRI_ALL_SESSIONS_PATTERN;
        _         -> ?KHEPRI_USER_SESSIONS_PATTERN(UsernameFilter)
    end,
    Now = os:system_time(millisecond),
    FilteredSessions = case rabbit_khepri:get_many(PathPattern) of
        {ok, Map} ->
            maps:fold(fun(Path, S, Acc) ->
                if is_record(S, session) andalso S#session.expires_at > Now ->
                       [rabbitmq, users, Username, sessions, SessionId] = Path,
                       [{Username, SessionId, S} | Acc];
                   true ->
                       Acc
                end
            end, [], Map);
        _ ->
            []
    end,
    Sorted = lists:sort(fun({_U1, _Id1, S1}, {_U2, _Id2, S2}) -> S1#session.created_at >= S2#session.created_at end, FilteredSessions),
    FilteredCount = length(Sorted),
    TotalCount = case UsernameFilter of
        undefined -> FilteredCount;
        _ ->
            case rabbit_khepri:get_many(?KHEPRI_ALL_SESSIONS_PATTERN) of
                {ok, AllMap} ->
                    maps:fold(fun(_P, S, Acc) ->
                        if is_record(S, session) andalso S#session.expires_at > Now ->
                               Acc + 1;
                           true ->
                               Acc
                        end
                    end, 0, AllMap);
                _ -> FilteredCount
            end
    end,
    Start = (Page - 1) * PageSize + 1,
    Items = if
        Start > FilteredCount -> [];
        true -> lists:sublist(Sorted, Start, PageSize)
    end,
    PageCount = if FilteredCount == 0 -> 0; true -> (FilteredCount + PageSize - 1) div PageSize end,
    ItemCount = length(Items),
    #{
        items => [session_to_map(Username, SessionId, S) || {Username, SessionId, S} <- Items],
        total_count => TotalCount,
        filtered_count => FilteredCount,
        item_count => ItemCount,
        page => Page,
        page_size => PageSize,
        page_count => PageCount
    }.

terminate_sessions(undefined) ->
    ok;
terminate_sessions(Username) ->
    _ = rabbit_khepri:delete_many(?KHEPRI_USER_SESSIONS_PATTERN(Username)),
    _ = rabbit_khepri:delete([rabbitmq, users, Username, sessions]),
    ?LOG_DEBUG("Terminated all sessions for user ~s", [Username]),
    ok.

%%====================================================================
%% gen_server callbacks
%%====================================================================

init([]) ->
    Timer = erlang:send_after(?SWEEP_INTERVAL, self(), sweep_expired_sessions),
    {ok, #state{timer = Timer}}.

handle_call(_Request, _From, State) ->
    {reply, ignored, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(sweep_expired_sessions, State) ->
    case ra_leaderboard:lookup_leader(rabbit_khepri:get_store_id()) of
        {_, Node} when Node == node() ->
            case sweep_expired_sessions_in_khepri() of
                0 -> ok;
                Count -> ?LOG_INFO("Swept ~b expired Management UI session(s)", [Count])
            end;
        _ ->
            ok
    end,
    Timer = erlang:send_after(?SWEEP_INTERVAL, self(), sweep_expired_sessions),
    {noreply, State#state{timer = Timer}};

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, State) ->
    if State#state.timer =/= undefined ->
        _ = erlang:cancel_timer(State#state.timer),
        ok;
       true -> ok
    end,
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%====================================================================
%% Internal Functions
%%====================================================================

sweep_expired_sessions_in_khepri() ->
    Now = os:system_time(millisecond),
    PathPattern = ?KHEPRI_ALL_SESSIONS_PATTERN,
    case rabbit_khepri:get_many(PathPattern) of
        {ok, Map} when map_size(Map) > 0 ->
            ExpiredPaths = maps:fold(fun(Path, S, Acc) ->
                if is_record(S, session) andalso Now > S#session.expires_at ->
                       [Path | Acc];
                   true ->
                       Acc
                end
            end, [], Map),
            case ExpiredPaths of
                [] ->
                    0;
                _ ->
                    lists:foreach(fun(Path) ->
                        _ = rabbit_khepri:delete(Path)
                    end, ExpiredPaths),
                    length(ExpiredPaths)
            end;
        _ ->
            0
    end.

calculate_expires_at(Now) ->
    calculate_expires_at(Now, Now).

calculate_expires_at(CreatedAt, Now) ->
    min(CreatedAt + session_timeout_ms(), Now + heartbeat_timeout_ms()).

session_timeout_ms() ->
    application:get_env(rabbitmq_management, login_session_timeout, 480) * 60 * 1000.

heartbeat_timeout_ms() ->
    Settings = rabbit_mgmt_features:get_sessions_settings(),
    HeartbeatIntervalSec = proplists:get_value(heartbeat_interval, Settings, 30),
    HeartbeatIntervalSec * 2 * 1000.

session_to_map(Username, SessionId, S) ->
    #{
        id => SessionId,
        username => Username,
        created_at => S#session.created_at,
        expires_at => S#session.expires_at,
        metadata => S#session.metadata
    }.
