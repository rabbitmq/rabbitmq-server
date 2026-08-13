%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%
-module(rabbit_mgmt_sessions).

-behaviour(gen_server).

-export([start_link/0]).
-export([create_session/2, heartbeat/2, delete_session/1,
         list_sessions/3, terminate_session_admin/1,
         terminate_sessions_for_user_admin/1]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-include_lib("kernel/include/logger.hrl").

-record(session, {
    id                   :: binary(),
    username             :: binary(),
    node                 :: atom(),
    created_at           :: integer(),
    expires_at           :: integer(),
    heartbeat_expires_at :: integer(),
    metadata             :: #{binary() => binary()}
}).

-record(state, {
    local_sessions  :: #{binary() => #session{}},
    remote_sessions :: #{atom() => [#session{}]},
    timer            :: reference()
}).

-define(BROADCAST_INTERVAL, 5000).

%%====================================================================
%% API
%%====================================================================

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

create_session(Username, Metadata) ->
    gen_server:call(?MODULE, {create_session, Username, Metadata}).

heartbeat(SessionId, Username) ->
    gen_server:call(?MODULE, {heartbeat, SessionId, Username}).

delete_session(SessionId) ->
    gen_server:cast(?MODULE, {delete_session, SessionId}).

list_sessions(Page, PageSize, UsernameFilter) ->
    gen_server:call(?MODULE, {list_sessions, Page, PageSize, UsernameFilter}).

terminate_session_admin(SessionId) ->
    gen_server:call(?MODULE, {terminate_session_admin, SessionId}).

terminate_sessions_for_user_admin(Username) ->
    gen_server:cast(?MODULE, {terminate_sessions_for_user_admin, Username}).

%%====================================================================
%% gen_server callbacks
%%====================================================================

init([]) ->
    Timer = erlang:send_after(?BROADCAST_INTERVAL, self(), broadcast_and_cleanup),
    {ok, #state{local_sessions = #{}, remote_sessions = #{}, timer = Timer}}.

handle_call({create_session, Username, Metadata}, _From, State) ->
    Settings = rabbit_mgmt_features:get_sessions_settings(),
    MaxConcurrent = proplists:get_value(max_concurrent, Settings, 1),
    Count = count_sessions_for_user(Username, State),
    if
        Count >= MaxConcurrent ->
            ?LOG_DEBUG("Failed to create session for user ~s: concurrent session limit reached", [Username]),
            {reply, {error, limit_reached}, State};
        true ->
            SessionId = list_to_binary(rabbit_guid:to_string(rabbit_guid:gen())),
            Now = os:system_time(millisecond),
            ExpiresAt = Now + session_timeout_ms(),
            HeartbeatExpiresAt = Now + heartbeat_timeout_ms(),
            Session = #session{
                id = SessionId,
                username = Username,
                node = node(),
                created_at = Now,
                expires_at = ExpiresAt,
                heartbeat_expires_at = HeartbeatExpiresAt,
                metadata = Metadata
            },
            NewLocalSessions = maps:put(SessionId, Session, State#state.local_sessions),
            NewState = State#state{local_sessions = NewLocalSessions},
            ?LOG_DEBUG("Created session ~s for user ~s on node ~s", [SessionId, Username, node()]),
            {reply, {ok, SessionId}, NewState}
    end;

handle_call({heartbeat, SessionId, Username}, _From, State) ->
    Now = os:system_time(millisecond),
    case maps:get(SessionId, State#state.local_sessions, undefined) of
        undefined ->
            %% Session might be remote, or terminated, or expired
            %% If it's a remote session, it shouldn't be heartbeating to this node usually, 
            %% but maybe a load balancer routed it here. We only accept heartbeats for local sessions?
            %% Wait, the plan says "terminate local session". 
            %% A heartbeat could hit any node. If it hits a node that didn't create the session,
            %% it might be a remote session. But the design implies sessions are sticky or local?
            %% Usually, the session is created on a node, and the heartbeat comes to the same node,
            %% or if it hits another node, we need to forward it? Let's check.
            %% "Heartbeat (ownership enforced) -> 200 or 401 / 403"
            %% Let's search remote sessions just in case. If we find it on a remote node, we could forward the heartbeat or reject.
            %% Let's just reject if it's not local? Actually if it's remote we might forward it. 
            %% Let's check if it exists in remote.
            case find_remote_session(SessionId, State#state.remote_sessions) of
                {ok, RemoteNode, RemoteSession} ->
                    if RemoteSession#session.username =/= Username ->
                            {reply, {error, forbidden}, State};
                       true ->
                            case rpc:call(RemoteNode, ?MODULE, heartbeat, [SessionId, Username]) of
                                ok -> {reply, ok, State};
                                {error, _} = Err -> {reply, Err, State};
                                _ -> {reply, {error, not_found}, State}
                            end
                    end;
                error ->
                    %% Auto-resume (adopt) the orphaned session
                    Settings = rabbit_mgmt_features:get_sessions_settings(),
                    MaxConcurrent = proplists:get_value(max_concurrent, Settings, 1),
                    Count = count_sessions_for_user(Username, State),
                    if
                        Count >= MaxConcurrent ->
                            {reply, {error, not_found}, State};
                        true ->
                            ExpiresAt = Now + session_timeout_ms(),
                            HeartbeatExpiresAt = Now + heartbeat_timeout_ms(),
                            Session = #session{
                                id = SessionId,
                                username = Username,
                                node = node(),
                                created_at = Now, %% Fresh timestamp makes it the first to be killed in conflicts
                                expires_at = ExpiresAt,
                                heartbeat_expires_at = HeartbeatExpiresAt,
                                metadata = #{} %% Adopted sessions start with empty metadata
                            },
                            NewLocalSessions = maps:put(SessionId, Session, State#state.local_sessions),
                            {reply, ok, State#state{local_sessions = NewLocalSessions}}
                    end
            end;
        Session ->
            if Session#session.username =/= Username ->
                    {reply, {error, forbidden}, State};
               true ->
                    HeartbeatExpiresAt = Now + heartbeat_timeout_ms(),
                    NewSession = Session#session{heartbeat_expires_at = HeartbeatExpiresAt},
                    NewLocalSessions = maps:put(SessionId, NewSession, State#state.local_sessions),
                    {reply, ok, State#state{local_sessions = NewLocalSessions}}
            end
    end;

handle_call({list_sessions, Page, PageSize, UsernameFilter}, _From, State) ->
    AllSessions = all_sessions(State),
    Filtered = case UsernameFilter of
        undefined -> AllSessions;
        _ -> [S || S <- AllSessions, S#session.username == UsernameFilter]
    end,
    %% Sort by created_at desc
    Sorted = lists:sort(fun(S1, S2) -> S1#session.created_at >= S2#session.created_at end, Filtered),
    TotalCount = length(Sorted),
    Start = (Page - 1) * PageSize + 1,
    Items = if
        Start > TotalCount -> [];
        true -> lists:sublist(Sorted, Start, PageSize)
    end,
    Result = #{
        items => [session_to_map(S) || S <- Items],
        total_count => TotalCount,
        page => Page,
        page_size => PageSize
    },
    {reply, Result, State};

handle_call({terminate_session_admin, SessionId}, _From, State) ->
    case maps:get(SessionId, State#state.local_sessions, undefined) of
        undefined ->
            case find_remote_session(SessionId, State#state.remote_sessions) of
                {ok, RemoteNode, _RemoteSession} ->
                    case rpc:call(RemoteNode, ?MODULE, terminate_session_admin, [SessionId]) of
                        ok -> {reply, ok, State};
                        {error, _} = Err -> {reply, Err, State};
                        _ -> {reply, {error, not_found}, State}
                    end;
                error ->
                    {reply, {error, not_found}, State}
            end;
        _Session ->
            NewLocalSessions = maps:remove(SessionId, State#state.local_sessions),
            {reply, ok, State#state{local_sessions = NewLocalSessions}}
    end;

handle_call(_Request, _From, State) ->
    {reply, ignored, State}.

handle_cast({terminate_sessions_for_user_admin, Username}, State) ->
    ?LOG_DEBUG("Admin terminated all sessions for user ~s", [Username]),
    %% Remove local sessions for this user
    NewLocalSessions = maps:filter(fun(_Id, S) ->
        S#session.username =/= Username
    end, State#state.local_sessions),
    
    %% Remove remote sessions for this user
    NewRemoteSessions = maps:map(fun(_Node, Sessions) ->
        [S || S <- Sessions, S#session.username =/= Username]
    end, State#state.remote_sessions),
    
    %% Broadcast to other nodes to do the same
    Msg = {terminate_sessions_for_user_admin_local, Username},
    lists:foreach(fun(N) ->
        if N =/= node() ->
            gen_server:cast({?MODULE, N}, Msg);
        true -> ok
        end
    end, nodes()),
    
    {noreply, State#state{local_sessions = NewLocalSessions, remote_sessions = NewRemoteSessions}};

handle_cast({terminate_sessions_for_user_admin_local, Username}, State) ->
    %% Remove local sessions for this user (received from broadcast)
    NewLocalSessions = maps:filter(fun(_Id, S) ->
        S#session.username =/= Username
    end, State#state.local_sessions),
    
    %% Remove remote sessions for this user
    NewRemoteSessions = maps:map(fun(_Node, Sessions) ->
        [S || S <- Sessions, S#session.username =/= Username]
    end, State#state.remote_sessions),
    
    {noreply, State#state{local_sessions = NewLocalSessions, remote_sessions = NewRemoteSessions}};

handle_cast({delete_session, SessionId}, State) ->
    %% Attempt to delete local. If not local, forward to remote.
    case maps:is_key(SessionId, State#state.local_sessions) of
        true ->
            ?LOG_DEBUG("Deleted session ~s", [SessionId]),
            NewLocalSessions = maps:remove(SessionId, State#state.local_sessions),
            {noreply, State#state{local_sessions = NewLocalSessions}};
        false ->
            case find_remote_session(SessionId, State#state.remote_sessions) of
                {ok, RemoteNode, _} ->
                    gen_server:cast({?MODULE, RemoteNode}, {delete_session, SessionId}),
                    {noreply, State};
                error ->
                    {noreply, State}
            end
    end;

handle_cast({session_summary, RemoteNode, RemoteSessionsList}, State) ->
    NewRemoteSessions = maps:put(RemoteNode, RemoteSessionsList, State#state.remote_sessions),
    State1 = State#state{remote_sessions = NewRemoteSessions},
    State2 = resolve_conflicts(State1),
    {noreply, State2};

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(broadcast_and_cleanup, State) ->
    Now = os:system_time(millisecond),
    
    %% Cleanup local
    LocalSessions = State#state.local_sessions,
    LocalSessions1 = maps:filter(fun(_Id, S) -> 
        (S#session.expires_at > Now) andalso (S#session.heartbeat_expires_at > Now) 
    end, LocalSessions),
    
    %% Cleanup remote
    RemoteSessions = State#state.remote_sessions,
    RemoteSessions1 = maps:map(fun(_Node, SessionsList) ->
        [S || S <- SessionsList, (S#session.expires_at > Now) andalso (S#session.heartbeat_expires_at > Now)]
    end, RemoteSessions),
    
    %% Also cleanup dead nodes
    ActiveNodes = nodes(),
    RemoteSessions2 = maps:filter(fun(Node, _List) -> lists:member(Node, ActiveNodes) end, RemoteSessions1),
    
    State1 = State#state{local_sessions = LocalSessions1, remote_sessions = RemoteSessions2},
    
    %% Broadcast
    LocalSessionsList = maps:values(LocalSessions1),
    Msg = {session_summary, node(), LocalSessionsList},
    lists:foreach(fun(N) ->
        gen_server:cast({?MODULE, N}, Msg)
    end, ActiveNodes),
    
    Timer = erlang:send_after(?BROADCAST_INTERVAL, self(), broadcast_and_cleanup),
    {noreply, State1#state{timer = Timer}};

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

session_timeout_ms() ->
    %% configured in minutes, convert to ms
    application:get_env(rabbitmq_management, login_session_timeout, 480) * 60 * 1000.

heartbeat_timeout_ms() ->
    Settings = rabbit_mgmt_features:get_sessions_settings(),
    HeartbeatIntervalSec = proplists:get_value(heartbeat_interval, Settings, 30),
    %% Allow 2 missed heartbeats (so 3 intervals total)
    HeartbeatIntervalSec * 3 * 1000.

count_sessions_for_user(Username, State) ->
    LocalCount = maps:fold(fun(_Id, S, Acc) ->
        if S#session.username == Username -> Acc + 1; true -> Acc end
    end, 0, State#state.local_sessions),
    RemoteCount = maps:fold(fun(_Node, Sessions, Acc) ->
        Acc + length([S || S <- Sessions, S#session.username == Username])
    end, 0, State#state.remote_sessions),
    LocalCount + RemoteCount.

all_sessions(State) ->
    Local = maps:values(State#state.local_sessions),
    Remote = lists:flatmap(fun(S) -> S end, maps:values(State#state.remote_sessions)),
    Local ++ Remote.

find_remote_session(SessionId, RemoteSessions) ->
    Res = maps:fold(fun(Node, Sessions, Acc) ->
        case Acc of
            error ->
                case lists:keyfind(SessionId, #session.id, Sessions) of
                    false -> error;
                    Session -> {ok, Node, Session}
                end;
            _ -> Acc
        end
    end, error, RemoteSessions),
    Res.

session_to_map(S) ->
    #{
        id => S#session.id,
        username => S#session.username,
        node => S#session.node,
        created_at => S#session.created_at,
        expires_at => S#session.expires_at,
        heartbeat_expires_at => S#session.heartbeat_expires_at,
        metadata => S#session.metadata
    }.

resolve_conflicts(State) ->
    Settings = rabbit_mgmt_features:get_sessions_settings(),
    MaxConcurrent = proplists:get_value(max_concurrent, Settings, 1),
    AllSessions = all_sessions(State),
    
    %% Group by username
    UserMap = lists:foldl(fun(S, Acc) ->
        U = S#session.username,
        List = maps:get(U, Acc, []),
        maps:put(U, [S | List], Acc)
    end, #{}, AllSessions),
    
    %% Find which local sessions need to be killed
    KillIds = maps:fold(fun(_User, Sessions, AccKill) ->
        if
            length(Sessions) > MaxConcurrent ->
                %% Sort to find which ones to kill. We keep the oldest (smallest created_at).
                %% If created_at ties, node name resolves it.
                %% We want to kill the *newest* sessions to get down to MaxConcurrent.
                %% So sort descending by created_at (newest first). 
                %% If tie, larger node name first (it gets killed).
                Sorted = lists:sort(fun(S1, S2) ->
                    if S1#session.created_at == S2#session.created_at ->
                        S1#session.node >= S2#session.node;
                    true ->
                        S1#session.created_at > S2#session.created_at
                    end
                end, Sessions),
                
                %% The first (length(Sessions) - MaxConcurrent) elements are the ones to kill
                ToKill = lists:sublist(Sorted, length(Sessions) - MaxConcurrent),
                %% We only kill our own local sessions
                MyNode = node(),
                LocalToKill = [S#session.id || S <- ToKill, S#session.node == MyNode],
                AccKill ++ LocalToKill;
            true ->
                AccKill
        end
    end, [], UserMap),
    
    NewLocalSessions = lists:foldl(fun(Id, Acc) -> maps:remove(Id, Acc) end, State#state.local_sessions, KillIds),
    State#state{local_sessions = NewLocalSessions}.
