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

-import(rabbit_federation_util, [name/1]).

-record(not_started, {queue, run, upstream, upstream_params}).
-record(state, {queue, run, conn, ch, dconn, dch, credit, ctag,
                upstream, upstream_params, unacked}).

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

init({Upstream, Queue = #amqqueue{name = QName}}) ->
    UParams = rabbit_federation_upstream:to_params(Upstream, Queue),
    rabbit_federation_status:report(Upstream, UParams, QName, starting),
    join(rabbit_federation_queues),
    join({rabbit_federation_queue, QName}),
    gen_server2:cast(self(), maybe_go),
    {ok, #not_started{queue           = Queue,
                      run             = false,
                      upstream        = Upstream,
                      upstream_params = UParams}}.

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
                                          credit       = ?CREDIT,
                                          drain        = false}),
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
                                          credit       = 0,
                                          drain        = false}),
    {noreply, State#state{run = false, credit = 0}};

handle_cast(basic_get, State = #not_started{}) ->
    {noreply, State};

handle_cast(basic_get, State = #state{ch = Ch, credit = Credit, ctag = CTag}) ->
    Credit1 = Credit + 1,
    amqp_channel:cast(
      Ch, #'basic.credit'{consumer_tag = CTag,
                          credit       = Credit1,
                          drain        = false}),
    {noreply, State#state{credit = Credit1}};

handle_cast(Msg, State) ->
    {stop, {unexpected_cast, Msg}, State}.

handle_info(#'basic.consume_ok'{}, State) ->
    {noreply, State};

handle_info(#'basic.ack'{} = Ack, State = #state{ch      = Ch,
                                                 unacked = Unacked}) ->
    Unacked1 = rabbit_federation_link_util:ack(Ack, Ch, Unacked),
    {noreply, State#state{unacked = Unacked1}};

handle_info(#'basic.nack'{} = Nack, State = #state{ch      = Ch,
                                                   unacked = Unacked}) ->
    Unacked1 = rabbit_federation_link_util:nack(Nack, Ch, Unacked),
    {noreply, State#state{unacked = Unacked1}};

handle_info({#'basic.deliver'{redelivered = Redelivered} = DeliverMethod, Msg},
            State = #state{queue           = #amqqueue{name = QName},
                           upstream        = Upstream,
                           upstream_params = UParams,
                           run             = Run,
                           credit          = Credit,
                           ctag            = CTag,
                           ch              = Ch,
                           dch             = DCh,
                           unacked         = Unacked}) ->
    PublishMethod = #'basic.publish'{exchange    = <<"">>,
                                     routing_key = QName#resource.name},
    HeadersFun = fun (H) -> update_headers(UParams, Redelivered, H) end,
    ForwardFun = fun (_H) -> true end,
    Unacked1 = rabbit_federation_link_util:forward(
                 Upstream, DeliverMethod, Ch, DCh, PublishMethod,
                 HeadersFun, ForwardFun, Msg, Unacked),
    %% TODO we could also hook this up to internal credit
    %% TODO actually we could reject when 'stopped'
    Credit1 = case Run of
                  false -> Credit - 1;
                  true  -> case Credit of
                               I when I < ?CREDIT_MORE_AT ->
                                   More = ?CREDIT - ?CREDIT_MORE_AT,
                                   amqp_channel:cast(
                                     Ch, #'basic.credit'{consumer_tag = CTag,
                                                         credit       = More,
                                                         drain        = false}),
                                   I - 1 + More;
                               I ->
                                   I - 1
                           end
              end,
    {noreply, State#state{credit  = Credit1,
                          unacked = Unacked1}};

handle_info(#'basic.cancel'{},
            State = #state{queue           = #amqqueue{name = QName},
                           upstream        = Upstream,
                           upstream_params = UParams}) ->
    rabbit_federation_link_util:connection_error(
      local, basic_cancel, Upstream, UParams, QName, State);

handle_info({'DOWN', _Ref, process, Pid, Reason},
            State = #state{dch             = DCh,
                           ch              = Ch,
                           upstream        = Upstream,
                           upstream_params = UParams,
                           queue           = #amqqueue{name = QName}}) ->
    rabbit_federation_link_util:handle_down(
      Pid, Reason, Ch, DCh, {Upstream, UParams, QName}, State);

handle_info(Msg, State) ->
    {stop, {unexpected_info, Msg}, State}.

terminate(_Reason, #not_started{}) ->
    ok;

terminate(Reason, #state{dconn           = DConn,
                         conn            = Conn,
                         upstream        = Upstream,
                         upstream_params = UParams,
                         queue           = #amqqueue{name = QName}}) ->
    rabbit_federation_link_util:ensure_connection_closed(DConn),
    rabbit_federation_link_util:ensure_connection_closed(Conn),
    rabbit_federation_link_util:log_terminate(Reason, Upstream, UParams, QName),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%----------------------------------------------------------------------------

go(S0 = #not_started{run             = Run,
                     upstream        = Upstream,
                     upstream_params = UParams,
                     queue           = Queue = #amqqueue{name = QName}}) ->
    #upstream_params{x_or_q = UQueue} = UParams,
    Credit = case Run of
                 true  -> ?CREDIT;
                 false -> 0
             end,
    Args = [{<<"x-priority">>, long,  -1},
            {<<"x-credit">>,   table, [{<<"credit">>, long, Credit},
                                       {<<"drain">>,  bool, false}]}],
    Unacked = rabbit_federation_link_util:unacked_new(),
    NoAck = Upstream#upstream.ack_mode =:= 'no-ack',
    rabbit_federation_link_util:start_conn_ch(
      fun (Conn, Ch, DConn, DCh) ->
              #'basic.consume_ok'{consumer_tag = CTag} =
                  amqp_channel:call(
                    Ch, #'basic.consume'{queue     = name(UQueue),
                                         no_ack    = NoAck,
                                         arguments = Args}),
              {noreply, #state{queue           = Queue,
                               run             = Run,
                               conn            = Conn,
                               ch              = Ch,
                               dconn           = DConn,
                               dch             = DCh,
                               credit          = Credit,
                               ctag            = CTag,
                               upstream        = Upstream,
                               upstream_params = UParams,
                               unacked         = Unacked}}
      end, Upstream, UParams, QName, S0).

update_headers(UParams, Redelivered, undefined) ->
    update_headers(UParams, Redelivered, []);

update_headers(UParams, Redelivered, Headers) ->
    {table, Info} = rabbit_federation_upstream:params_to_table(UParams),
    {Headers1, Count} =
        case rabbit_misc:table_lookup(Headers, ?ROUTING_HEADER) of
            undefined ->
                {Headers, 0};
            {array, Been} ->
                {Found, Been1} = lists:partition(
                                      fun (I) -> visit_match(I, Info) end,
                                      Been),
                C = case Found of
                        []           -> 0;
                        [{table, T}] -> case rabbit_misc:table_lookup(
                                               T, <<"visit-count">>) of
                                            {_, I} when is_number(I) -> I;
                                            _                        -> 0
                                        end
                    end,
                {rabbit_misc:set_table_value(
                   Headers, ?ROUTING_HEADER, array, Been1), C}
        end,
    rabbit_basic:prepend_table_header(
      ?ROUTING_HEADER, Info ++ [{<<"redelivered">>, bool, Redelivered},
                                {<<"visit-count">>, long, Count + 1}],
      Headers1).

visit_match({table, T}, Info) ->
    lists:all(fun (K) ->
                      rabbit_misc:table_lookup(T, K) =:=
                          rabbit_misc:table_lookup(Info, K)
              end, [<<"uri">>, <<"virtual_host">>, <<"queue">>]);
visit_match(_ ,_) ->
    false.
