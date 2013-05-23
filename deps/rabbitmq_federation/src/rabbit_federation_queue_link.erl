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

-export([start_link/1, go/0, run/1, stop/1, basic_get/1]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(not_started, {queue, run, params}).
-record(state, {queue, run, conn, ch, dconn, dch, credit, ctag}).

%% TODO make this configurable
-define(CREDIT, 200).
-define(CREDIT_MORE_AT, 50).

start_link(Args) ->
    gen_server2:start_link(?MODULE, Args, [{timeout, infinity}]).

%% TODO on restart we will lose stopped / started state!
run(QName)       -> cast(QName, run).
stop(QName)      -> cast(QName, stop).
basic_get(QName) -> cast(QName, basic_get).
go()             -> cast(go).

%%----------------------------------------------------------------------------
%%call(QName, Msg) -> [gen_server2:call(Pid, Msg, infinity) || Pid <- q(QName)].
cast(Msg)        -> [gen_server2:cast(Pid, Msg) || Pid <- all()].
cast(QName, Msg) -> [gen_server2:cast(Pid, Msg) || Pid <- q(QName)].

join(Name) ->
    pg2_fixed:create(Name),
    ok = pg2_fixed:join(Name, self()).

all() ->
    pg2_fixed:create(rabbit_federation_queues),
    pg2_fixed:get_members(rabbit_federation_queues).

q(QName) ->
    pg2_fixed:create({rabbit_federation_queue, QName}),
    pg2_fixed:get_members({rabbit_federation_queue, QName}).

federation_up() ->
    proplists:is_defined(rabbitmq_federation,
                         application:which_applications(infinity)).

%%----------------------------------------------------------------------------

init({#upstream_params{params = Params}, Queue = #amqqueue{name = QName}}) ->
    join(rabbit_federation_queues),
    join({rabbit_federation_queue, QName}),
    gen_server2:cast(self(), maybe_go),
    {ok, #not_started{queue  = Queue,
                      run    = false,
                      params = Params}}.

handle_call(Msg, _From, State) ->
    {stop, {unexpected_call, Msg}, State}.

handle_cast(maybe_go, State) ->
    case federation_up() of
        true  -> go(State);
        false -> {noreply, State}
    end;

handle_cast(go, State = #not_started{}) ->
    go(State);

handle_cast(go, State) ->
    {noreply, State};

handle_cast(run, State = #state{ch = Ch, run = false, ctag = CTag}) ->
    amqp_channel:cast(Ch, #'basic.credit'{consumer_tag = CTag,
                                          credit       = ?CREDIT}),
    {noreply, State#state{run = true, credit = ?CREDIT}};

handle_cast(run, State = #not_started{}) ->
    {noreply, State#not_started{run = true}};

handle_cast(run, State) ->
    %% Already started
    {noreply, State};

handle_cast(stop, State = #state{run = false}) ->
    %% Already stopped
    {noreply, State};

handle_cast(stop, State = #not_started{}) ->
    {noreply, State#not_started{run = false}};

handle_cast(stop, State = #state{ch = Ch, ctag = CTag}) ->
    amqp_channel:cast(Ch, #'basic.credit'{consumer_tag = CTag,
                                          credit       = 0}),
    {noreply, State#state{run = false, credit = 0}};

handle_cast(basic_get, State = #not_started{}) ->
    {noreply, State};

handle_cast(basic_get, State = #state{ch = Ch, credit = Credit, ctag = CTag}) ->
    Credit1 = Credit + 1,
    amqp_channel:cast(
      Ch, #'basic.credit'{consumer_tag = CTag,
                          credit       = Credit1}),
    {noreply, State#state{credit = Credit1}};

handle_cast(Msg, State) ->
    {stop, {unexpected_cast, Msg}, State}.

handle_info(#'basic.consume_ok'{}, State) ->
    {noreply, State};

handle_info({#'basic.deliver'{}, Msg},
            State = #state{queue  = #amqqueue{name = QName},
                           run         = Run,
                           credit      = Credit,
                           ctag        = CTag,
                           ch          = Ch,
                           dch         = DCh}) ->
    %% TODO share with _link
    amqp_channel:cast(DCh, #'basic.publish'{exchange    = <<"">>,
                                            routing_key = QName#resource.name},
                      Msg),
    %% TODO we could also hook this up to internal credit
    %% TODO actually we could reject when 'stopped'
    Credit1 = case Run of
                  false -> Credit - 1;
                  true  -> case Credit of
                               I when I < ?CREDIT_MORE_AT ->
                                   More = ?CREDIT - ?CREDIT_MORE_AT,
                                   amqp_channel:cast(
                                     Ch, #'basic.credit'{consumer_tag = CTag,
                                                         credit       = More}),
                                   I - 1 + More;
                               I ->
                                   I - 1
                           end
              end,
    {noreply, State#state{credit = Credit1}};

handle_info(Msg, State) ->
    {stop, {unexpected_info, Msg}, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%----------------------------------------------------------------------------

go(#not_started{run       = Run,
                params    = Params,
                queue     = Queue = #amqqueue{name = QName}}) ->
    Credit = case Run of
                 true  -> ?CREDIT;
                 false -> 0
             end,
    %% TODO monitor and share code with _link
    {ok, Conn} = amqp_connection:start(Params),
    {ok, Ch} = amqp_connection:open_channel(Conn),
    {ok, DConn} = amqp_connection:start(#amqp_params_direct{}),
    {ok, DCh} = amqp_connection:open_channel(DConn),
    #'basic.consume_ok'{consumer_tag = CTag} =
        amqp_channel:call(Ch, #'basic.consume'{
                            queue     = QName#resource.name,
                            no_ack    = true,
                            arguments = [{<<"x-purpose">>, longstr, <<"federation">>},
                                         {<<"x-credit">>, table,
                                          [{<<"credit">>, long, Credit},
                                           {<<"drain">>,  bool, false}]}]}),
    {noreply, #state{queue     = Queue,
                     run       = Run,
                     conn      = Conn,
                     ch        = Ch,
                     dconn     = DConn,
                     dch       = DCh,
                     credit    = Credit,
                     ctag      = CTag}}.
