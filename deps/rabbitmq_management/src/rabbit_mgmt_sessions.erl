%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%
-module(rabbit_mgmt_sessions).

-behaviour(gen_server).

-export([start_link/0]).
-export([create_session/3, touch/3, delete_session/2,
         list_sessions/3, terminate_sessions/1]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-include_lib("kernel/include/logger.hrl").
-include_lib("khepri/include/khepri.hrl").

-record(state, {
    timer :: reference() | undefined
}).

-define(SWEEP_INTERVAL, 5000).

-define(KHEPRI_USER_SESSIONS_PATTERN(Username), [rabbitmq, mgmt_sessions, Username, ?KHEPRI_WILDCARD_STAR]).
-define(KHEPRI_SESSION_PATH(Username, SessionId), [rabbitmq, mgmt_sessions, Username, SessionId]).
-define(KHEPRI_ALL_SESSIONS_PATTERN, [rabbitmq, mgmt_sessions, ?KHEPRI_WILDCARD_STAR, ?KHEPRI_WILDCARD_STAR]).
-define(KHEPRI_SESSION_ID_PATTERN(SessionId), [rabbitmq, mgmt_sessions, ?KHEPRI_WILDCARD_STAR, SessionId]).

%%====================================================================
%% API
%%====================================================================

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

create_session(Username, Metadata, TokenExpiry) ->
    SessionId = binary:encode_hex(crypto:strong_rand_bytes(16), lowercase),
    Now = os:system_time(millisecond),
    SessionTimeoutMs = session_timeout_ms(),
    HeartbeatTimeoutMs = heartbeat_timeout_ms(),
    ExpiresAt = calculate_expires_at(Now, Now, SessionTimeoutMs, HeartbeatTimeoutMs, TokenExpiry),
    Session = #{
        created_at => Now,
        expires_at => ExpiresAt,
        metadata => Metadata
    },
    Settings = rabbit_mgmt_features:get_sessions_settings(),
    MaxConcurrent = proplists:get_value(max_concurrent, Settings, 1),
    UserSessionsPath = ?KHEPRI_USER_SESSIONS_PATTERN(Username),
    SessionPath = ?KHEPRI_SESSION_PATH(Username, SessionId),
    UserPath = rabbit_db_user:khepri_user_path(Username),

    TxRes = rabbit_khepri:transaction(fun() ->
        Map = case khepri_tx:get_many(UserSessionsPath) of
            {ok, M} -> M;
            _       -> #{}
        end,
        ActiveSessions = maps:fold(fun(P, S, Acc) ->
            case is_active(S, Now) of
                true ->
                    [S | Acc];
                false ->
                    ok = khepri_tx:delete(P),
                    Acc
            end
        end, [], Map),
        if length(ActiveSessions) >= MaxConcurrent ->
            {error, limit_reached};
           true ->
            PutOptions = case khepri_tx:exists(UserPath) of
                true  -> #{keep_while => #{UserPath => #if_node_exists{exists = true}}};
                false -> #{}
            end,
            case khepri_tx:put(SessionPath, Session, PutOptions) of
                ok               -> {ok, SessionId};
                {error, _} = Err -> Err
            end
        end
    end),
    case TxRes of
        {ok, SessionId} ->
            ?LOG_DEBUG("Created session ~ts for user ~ts", [SessionId, Username]),
            {ok, SessionId};
        {error, limit_reached} ->
            ?LOG_DEBUG("Failed to create session for user ~ts: concurrent session limit reached", [Username]),
            {error, limit_reached};
        {error, Reason} ->
            {error, Reason}
    end.

touch(undefined, _Username, _TokenExpiry) ->
    {error, not_found};
touch(_SessionId, undefined, _TokenExpiry) ->
    {error, not_found};
touch(SessionId, Username, TokenExpiry) ->
    Now = os:system_time(millisecond),
    SessionTimeoutMs = session_timeout_ms(),
    HeartbeatTimeoutMs = heartbeat_timeout_ms(),
    SessionPath = ?KHEPRI_SESSION_PATH(Username, SessionId),

    rabbit_khepri:transaction(fun() ->
        case khepri_tx:get(SessionPath) of
            {ok, Session} when is_map(Session) ->
                case is_active(Session, Now) of
                    true ->
                        NewExpiresAt = calculate_expires_at(
                            maps:get(created_at, Session, Now), Now, SessionTimeoutMs, HeartbeatTimeoutMs,
                            TokenExpiry),
                        NewSession = Session#{expires_at => NewExpiresAt},
                        khepri_tx:put(SessionPath, NewSession);
                    false ->
                        {error, not_found}
                end;
            _ ->
                PathPattern = ?KHEPRI_SESSION_ID_PATTERN(SessionId),
                case khepri_tx:get_many(PathPattern) of
                    {ok, Map} when map_size(Map) > 0 ->
                        ActiveOther = maps:fold(fun(_P, S, Acc) ->
                            case is_active(S, Now) of
                                true ->
                                    true;
                                false ->
                                    Acc
                            end
                        end, false, Map),
                        if ActiveOther ->
                            {error, forbidden};
                           true ->
                            {error, not_found}
                        end;
                    _ ->
                        {error, not_found}
                end
        end
    end).

delete_session(undefined, _Username) ->
    {error, not_found};
delete_session(SessionId, undefined) ->
    PathPattern = ?KHEPRI_SESSION_ID_PATTERN(SessionId),
    case rabbit_khepri:get_many(PathPattern) of
        {ok, Map} when map_size(Map) > 0 ->
            lists:foreach(fun rabbit_khepri:delete/1, maps:keys(Map)),
            ok;
        _ ->
            {error, not_found}
    end;
delete_session(SessionId, Username) ->
    SessionPath = ?KHEPRI_SESSION_PATH(Username, SessionId),
    case rabbit_khepri:get(SessionPath) of
        {ok, Session} when is_map(Session) ->
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
                case is_active(S, Now) of
                    true ->
                        [rabbitmq, mgmt_sessions, Username, SessionId] = Path,
                        [{Username, SessionId, S} | Acc];
                    false ->
                        Acc
                end
            end, [], Map);
        _ ->
            []
    end,
    Sorted = lists:sort(fun({_U1, _Id1, S1}, {_U2, _Id2, S2}) -> created_at(S1) >= created_at(S2) end, FilteredSessions),
    FilteredCount = length(Sorted),
    TotalCount = case UsernameFilter of
        undefined -> FilteredCount;
        _ ->
            case rabbit_khepri:get_many(?KHEPRI_ALL_SESSIONS_PATTERN) of
                {ok, AllMap} ->
                    maps:fold(fun(_P, S, Acc) ->
                        case is_active(S, Now) of
                            true ->
                                Acc + 1;
                            false ->
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
    _ = rabbit_khepri:delete([rabbitmq, mgmt_sessions, Username]),
    ?LOG_DEBUG("Terminated all sessions for user ~ts", [Username]),
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
                case is_expired(S, Now) of
                    true ->
                        [Path | Acc];
                    false ->
                        Acc
                end
            end, [], Map),
            case ExpiredPaths of
                [] ->
                    0;
                _ ->
                    lists:foreach(fun rabbit_khepri:delete/1, ExpiredPaths),
                    length(ExpiredPaths)
            end;
        _ ->
            0
    end.

is_active(S, Now) ->
    is_map(S) andalso maps:get(expires_at, S, 0) > Now.

is_expired(S, Now) ->
    is_map(S) andalso Now > maps:get(expires_at, S, 0).

created_at(S) ->
    maps:get(created_at, S, 0).

calculate_expires_at(CreatedAt, Now, SessionTimeoutMs, HeartbeatTimeoutMs, never) ->
    min(CreatedAt + SessionTimeoutMs, Now + HeartbeatTimeoutMs);
calculate_expires_at(CreatedAt, Now, SessionTimeoutMs, HeartbeatTimeoutMs, TokenExpiry) ->
    min(calculate_expires_at(CreatedAt, Now, SessionTimeoutMs, HeartbeatTimeoutMs, never),
        TokenExpiry * 1000).

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
        created_at => created_at(S),
        expires_at => maps:get(expires_at, S, 0),
        metadata => maps:get(metadata, S, #{})
    }.
