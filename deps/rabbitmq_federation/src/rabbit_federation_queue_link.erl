%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at http://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and
%% limitations under the License.
%%
%% The Original Code is RabbitMQ Federation.
%%
%% The Initial Developer of the Original Code is VMware, Inc.
%% Copyright (c) 2007-2013 VMware, Inc.  All rights reserved.
%%

-module(rabbit_federation_queue_link).

-include_lib("amqp_client/include/amqp_client.hrl").
-include("rabbit_federation.hrl").

-behaviour(gen_server2).

-export([start_link/3, go/1, stop/1]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {queue, coordinator, conn, ch, dconn, dch, credit, ctag}).

-define(CREDIT, 200).
-define(CREDIT_MORE_AT, 50).

%% TODO the whole supervisor thing
start_link(Params, Coordinator, Queue) ->
    gen_server2:start_link(?MODULE, {Params, Coordinator, Queue},
                           [{timeout, infinity}]).

go(Pid)   -> gen_server2:cast(Pid, go).
stop(Pid) -> gen_server2:cast(Pid, stop).

%%----------------------------------------------------------------------------

init({#upstream_params{params = Params}, Coordinator, Queue}) ->
    %% TODO monitor and share code with _link
    erlang:send_after(5000, self(), {start, Params}),
    {ok, #state{queue       = Queue,
                coordinator = Coordinator,
                credit      = {not_started, 0}}}.

handle_call(Msg, _From, State) ->
    {stop, {unexpected_call, Msg}, State}.

handle_cast(go, State = #state{ch = Ch, credit = stopped, ctag = CTag}) ->
    %%io:format("go!~n"),
    amqp_channel:cast(Ch, #'basic.credit'{consumer_tag = CTag,
                                          credit       = ?CREDIT}),
    {noreply, State#state{credit = ?CREDIT}};

%% TODO this hack should go away when we are supervised...
handle_cast(go, State = #state{credit = {not_started, _}}) ->
    {noreply, State#state{credit = {not_started, ?CREDIT}}};

handle_cast(go, State) ->
    %%io:format("ignore go!~n"),
    %% Already going
    {noreply, State};

handle_cast(stop, State = #state{credit = stopped}) ->
    %% Already stopped
    {noreply, State};

handle_cast(stop, State = #state{ch = Ch, ctag = CTag}) ->
    amqp_channel:cast(Ch, #'basic.credit'{consumer_tag = CTag,
                                          credit       = 0}),
    {noreply, State#state{credit = stopped}};

handle_cast(Msg, State) ->
    {stop, {unexpected_cast, Msg}, State}.

handle_info({start, Params}, State = #state{queue = #amqqueue{name = QName},
                                            credit = {not_started, Credit}}) ->
    {ok, Conn} = amqp_connection:start(Params),
    {ok, Ch} = amqp_connection:open_channel(Conn),
    {ok, DConn} = amqp_connection:start(#amqp_params_direct{}),
    {ok, DCh} = amqp_connection:open_channel(DConn),
    #'basic.consume_ok'{consumer_tag = CTag} =
        amqp_channel:call(Ch, #'basic.consume'{
                            queue     = QName#resource.name,
                            no_ack    = true,
                            arguments = [{<<"x-credit">>, table,
                                          [{<<"credit">>, long, Credit},
                                           {<<"drain">>,  bool, false}]}]}),
    {noreply, State#state{conn = Conn, ch = Ch,
                          dconn = DConn, dch = DCh,
                          credit = case Credit of
                                       0 -> stopped;
                                       _ -> Credit
                                   end, ctag = CTag}};

handle_info(#'basic.consume_ok'{}, State) ->
    {noreply, State};

handle_info({#'basic.deliver'{}, Msg},
            State = #state{queue  = #amqqueue{name = QName},
                           credit = Credit,
                           ctag   = CTag,
                           ch     = Ch,
                           dch    = DCh}) ->
    %% TODO share with _link
    amqp_channel:cast(DCh, #'basic.publish'{exchange    = <<"">>,
                                            routing_key = QName#resource.name},
                      Msg),
    %% TODO we could also hook this up to internal credit
    %% TODO actually we could reject when 'stopped'
    Credit1 = case Credit of
                  stopped ->
                      stopped;
                  I when I < ?CREDIT_MORE_AT ->
                      More = ?CREDIT - ?CREDIT_MORE_AT,
                      amqp_channel:cast(Ch, #'basic.credit'{consumer_tag = CTag,
                                                            credit       = More}),
                      I - 1 + More;
                  I ->
                      I - 1
              end,
    {noreply, State#state{credit = Credit1}};

handle_info(Msg, State) ->
    {stop, {unexpected_info, Msg}, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
