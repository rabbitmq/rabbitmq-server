%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_error_logger_handler).

-behaviour(gen_event).

%% API
-export([start_link/0, add_handler/0]).

%% gen_event callbacks
-export([init/1, handle_event/2, handle_call/2,
         handle_info/2, terminate/2, code_change/3]).

-define(SERVER, ?MODULE).

-record(state, {report = []}).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Creates an event manager
%%
%% @spec start_link() -> {ok, Pid} | {error, Error}
%% @end
%%--------------------------------------------------------------------
start_link() ->
    gen_event:start_link({local, ?SERVER}).

%%--------------------------------------------------------------------
%% @doc
%% Adds an event handler
%%
%% @spec add_handler() -> ok | {'EXIT', Reason} | term()
%% @end
%%--------------------------------------------------------------------
add_handler() ->
    gen_event:add_handler(?SERVER, ?MODULE, []).

%%%===================================================================
%%% gen_event callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Whenever a new event handler is added to an event manager,
%% this function is called to initialize the event handler.
%%
%% @spec init(Args) -> {ok, State}
%% @end
%%--------------------------------------------------------------------
init([]) ->
    {ok, #state{}}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Whenever an event manager receives an event sent using
%% gen_event:notify/2 or gen_event:sync_notify/2, this function is
%% called for each installed event handler to handle the event.
%%
%% @spec handle_event(Event, State) ->
%%                          {ok, State} |
%%                          {swap_handler, Args1, State1, Mod2, Args2} |
%%                          remove_handler
%% @end
%%--------------------------------------------------------------------

handle_event({info_report, _Gleader, {_Pid, _Type,
                                      {net_kernel, {'EXIT', _, Reason}}}},
             #state{report = Report} = State) ->
    NewReport = case format(Reason) of
                    [] -> Report;
                    Formatted -> [Formatted | Report]
                end,
    {ok, State#state{report = NewReport}};
handle_event(_Event, State) ->
    {ok, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Whenever an event manager receives a request sent using
%% gen_event:call/3,4, this function is called for the specified
%% event handler to handle the request.
%%
%% @spec handle_call(Request, State) ->
%%                   {ok, Reply, State} |
%%                   {swap_handler, Reply, Args1, State1, Mod2, Args2} |
%%                   {remove_handler, Reply}
%% @end
%%--------------------------------------------------------------------
handle_call(get_connection_report, State) ->
    {ok, lists:reverse(State#state.report), State#state{report = []}};
handle_call(_Request, State) ->
    Reply = ok,
    {ok, Reply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called for each installed event handler when
%% an event manager receives any other message than an event or a
%% synchronous request (or a system message).
%%
%% @spec handle_info(Info, State) ->
%%                         {ok, State} |
%%                         {swap_handler, Args1, State1, Mod2, Args2} |
%%                         remove_handler
%% @end
%%--------------------------------------------------------------------
handle_info(_Info, State) ->
    {ok, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Whenever an event handler is deleted from an event manager, this
%% function is called. It should be the opposite of Module:init/1 and
%% do any necessary cleaning up.
%%
%% @spec terminate(Reason, State) -> void()
%% @end
%%--------------------------------------------------------------------
terminate(_Reason, _State) ->
    ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%%
%% @spec code_change(OldVsn, State, Extra) -> {ok, NewState}
%% @end
%%--------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
format({check_dflag_xnc_failed, _What}) ->
    {"  * Remote node uses an incompatible Erlang version ~n", []};
format({recv_challenge_failed, no_node, Node}) ->
    {"  * Node name (or hostname) mismatch: node ~p believes its node name is not ~p but something else.~n"
     "    All nodes and CLI tools must refer to node ~p using the same name the node itself uses (see its logs to find out what it is)~n",
     [Node, Node, Node]};
format({recv_challenge_failed, Error}) ->
    {"  * Distribution failed unexpectedly while waiting for challenge: ~p~n", [Error]};
format({recv_challenge_ack_failed, bad_cookie}) ->
    {"  * Authentication failed (rejected by the local node), please check the Erlang cookie~n", []};
format({recv_challenge_ack_failed, {error, closed}}) ->
    {"  * Authentication failed (rejected by the remote node), please check the Erlang cookie~n", []};
format({recv_status_failed, not_allowed}) ->
    {"  * This node is not on the list of nodes authorised by remote node (see net_kernel:allow/1)~n", []};
format({recv_status_failed, {error, closed}}) ->
    {"  * Remote host closed TCP connection before completing authentication. Is the Erlang distribution using TLS?~n", []};
format(setup_timer_timeout) ->
    {"  * TCP connection to remote host has timed out. Is the Erlang distribution using TLS?~n", []};
format(_) ->
    [].
