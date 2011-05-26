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
%% Copyright (c) 2007-2011 VMware, Inc.  All rights reserved.
%%

-module(rabbit_federation_link).

-include_lib("amqp_client/include/amqp_client.hrl").
-include("rabbit_federation.hrl").

-behaviour(gen_server2).

-export([start_link/1]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-export([add_routing_to_headers/2]).

-define(ROUTING_HEADER, <<"x-forwarding">>).

-record(state, {upstream,
                connection,
                channel,
                queue,
                internal_exchange,
                waiting_cmds = gb_trees:empty(),
                bindings = dict:new(),
                downstream_connection,
                downstream_channel,
                downstream_exchange}).

%%----------------------------------------------------------------------------

start_link(Args) ->
    gen_server2:start_link(?MODULE, Args, [{timeout, infinity}]).

%%----------------------------------------------------------------------------

init(Args = {_, X}) ->
    rabbit_federation_links:join(rabbit_federation_exchanges),
    rabbit_federation_links:join({rabbit_federation_exchange, X}),
    gen_server2:cast(self(), maybe_go),
    {ok, {not_started, Args}}.

handle_call({enqueue, _, _}, _From, State = {not_started, _}) ->
    {reply, ok, State};

handle_call({enqueue, Serial, Cmd}, From,
            State = #state{waiting_cmds = Waiting}) ->
    Waiting1 = gb_trees:insert(Serial, Cmd, Waiting),
    {reply, ok,
     play_back_commands(Serial, From, State#state{waiting_cmds = Waiting1})};

handle_call(stop, _From, State = #state{connection = Conn, queue = Q}) ->
    disposable_channel_call(Conn, #'queue.delete'{queue = Q}),
    {stop, normal, ok, State};

handle_call(Msg, _From, State) ->
    {stop, {unexpected_call, Msg}, State}.

handle_cast(maybe_go, S0 = {not_started, _Args}) ->
    case rabbit_federation_util:federation_up() of
        true  -> go(S0);
        false -> {noreply, S0}
    end;

handle_cast(go, S0 = {not_started, _Args}) ->
    go(S0);

%% There's a small race - I think we can realise federation is up
%% before go_all gets invoked. Ignore.
handle_cast(go, State) ->
    {noreply, State};

handle_cast(Msg, State) ->
    {stop, {unexpected_cast, Msg}, State}.

handle_info(#'basic.consume_ok'{}, State) ->
    {noreply, State};

handle_info(#'basic.ack'{ delivery_tag = Seq, multiple = Multiple },
            State = #state{channel = Ch}) ->
    %% We rely on the fact that the delivery tags allocated by the
    %% consuming side will always be increasing from 1, the same as
    %% the publish sequence numbers are. This behaviour is not
    %% guaranteed by the spec, but it's what Rabbit does. Assuming
    %% this allows us to cut out a bunch of bookkeeping.
    amqp_channel:cast(Ch, #'basic.ack'{delivery_tag = Seq,
                                       multiple     = Multiple}),
    {noreply, State};

handle_info({#'basic.deliver'{%% TODO do we care?
                              %%redelivered = Redelivered,
                              %%exchange = Exchange,
                              routing_key = Key}, Msg},
            State = #state{upstream            = Upstream,
                           downstream_exchange = #resource{name = X},
                           downstream_channel  = DCh}) ->
    Headers0 = extract_headers(Msg),
    %% TODO add user information here?
    case forwarded_before(Headers0) of
        false -> Info = upstream_info(Upstream),
                 Headers = add_routing_to_headers(Headers0, Info),
                 amqp_channel:cast(DCh, #'basic.publish'{exchange    = X,
                                                         routing_key = Key},
                                   update_headers(Headers, Msg)),
                 ok;
        true  -> ok
    end,
    {noreply, State};

handle_info({'DOWN', _Ref, process, Ch, Reason},
            State = #state{ downstream_channel = DCh }) ->
    case Ch of
        DCh -> {stop, {downstream_channel_down, Reason}, State};
        _   -> {stop, {upstream_channel_down, Reason}, State}
    end;

handle_info(Msg, State) ->
    {stop, {unexpected_info, Msg}, State}.

terminate(_Reason, {not_started, _}) ->
    ok;

terminate(_Reason, #state{downstream_channel    = DCh,
                          downstream_connection = DConn,
                          connection            = Conn,
                          channel               = Ch}) ->
    ensure_closed(DConn, DCh),
    ensure_closed(Conn, Ch),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%----------------------------------------------------------------------------

handle_command({add_binding, Binding}, _From, State) ->
    add_binding(Binding, State);

handle_command({remove_binding, Binding}, _From, State) ->
    remove_binding(Binding, State).

play_back_commands(Serial, From, State = #state{waiting_cmds = Waiting}) ->
    case gb_trees:is_empty(Waiting) of
        false -> case gb_trees:take_smallest(Waiting) of
                     {Serial, Cmd, Waiting1} ->
                         State1 = State#state{waiting_cmds = Waiting1},
                         State2 = handle_command(Cmd, From, State1),
                         play_back_commands(Serial + 1, From, State2);
                     _ ->
                         State
                 end;
        true  -> State
    end.

open(Params) ->
    case amqp_connection:start(Params) of
        {ok, Conn} -> case amqp_connection:open_channel(Conn) of
                          {ok, Ch} -> erlang:monitor(process, Ch),
                                      {ok, Conn, Ch};
                          E        -> catch amqp_connection:close(Conn),
                                      E
                      end;
        E -> E
    end.

add_binding(B = #binding{key = Key, args = Args},
            State = #state{channel = Ch, internal_exchange = InternalX,
                           upstream = #upstream{exchange = X}}) ->
    case check_add_binding(B, State) of
        {true,  State1} -> ok;
        {false, State1} -> amqp_channel:call(
                             Ch, #'exchange.bind'{destination = InternalX,
                                                  source      = X,
                                                  routing_key = Key,
                                                  arguments   = Args})
    end,
    State1.

check_add_binding(B = #binding{destination = Dest},
                  State = #state{bindings = Bs}) ->
    K = key(B),
    {Res, Set} = case dict:find(K, Bs) of
                     {ok, Dests} -> {true,  sets:add_element(Dest, Dests)};
                     error       -> {false, sets:from_list([Dest])}
                 end,
    {Res, State#state{bindings = dict:store(K, Set, Bs)}}.

remove_binding(B = #binding{key = Key, args = Args},
               State = #state{channel = Ch, internal_exchange = InternalX,
                              upstream = #upstream{exchange = X}}) ->
    case check_remove_binding(B, State) of
        {true,  State1} -> ok;
        {false, State1} -> amqp_channel:call(
                             Ch, #'exchange.unbind'{destination = InternalX,
                                                    source      = X,
                                                    routing_key = Key,
                                                    arguments   = Args})
    end,
    State1.

check_remove_binding(B = #binding{destination = Dest},
                     State = #state{bindings = Bs}) ->
    K = key(B),
    Dests = sets:del_element(Dest, dict:fetch(K, Bs)),
    case sets:size(Dests) of
        0 -> {false, State#state{bindings = dict:erase(K, Bs)}};
        _ -> {true,  State#state{bindings = dict:store(K, Dests, Bs)}}
    end.

key(#binding{source = Source, key = Key, args = Args}) -> {Source, Key, Args}.

go(S0 = {not_started, {Upstream, #exchange{name    = DownstreamX,
                                           durable = Durable}}}) ->
    case open(rabbit_federation_util:local_params()) of
        {ok, DConn, DCh} ->
            #'confirm.select_ok'{} =
               amqp_channel:call(DCh, #'confirm.select'{}),
            amqp_channel:register_confirm_handler(DCh, self()),
            case open(Upstream#upstream.params) of
                {ok, Conn, Ch} ->
                    State = #state{downstream_connection = DConn,
                                   downstream_channel    = DCh,
                                   downstream_exchange   = DownstreamX,
                                   upstream              = Upstream,
                                   connection            = Conn,
                                   channel               = Ch},
                    State1 = consume_from_upstream_queue(State, Durable),
                    State2 = ensure_upstream_bindings(State1),
                    {noreply, State2};
                E ->
                    ensure_closed(DConn, DCh),
                    {stop, E, S0}
            end;
        E ->
            {stop, E, S0}
    end.

consume_from_upstream_queue(State = #state{upstream            = Upstream,
                                           connection          = Conn,
                                           channel             = Ch,
                                           downstream_exchange = DownstreamX},
                            Durable) ->
    #upstream{exchange       = X,
              prefetch_count = PrefetchCount,
              queue_expires  = Expiry,
              params         = #amqp_params_network{virtual_host = VHost}}
        = Upstream,
    Q = upstream_queue_name(X, VHost, DownstreamX),
    case Durable of
        false -> delete_upstream_queue(Conn, Q);
        _     -> ok
    end,
    amqp_channel:call(
      Ch, #'queue.declare'{
        queue     = Q,
        durable   = true,
        arguments = [{<<"x-expires">>, long, Expiry * 1000},
                     rabbit_federation_util:purpose_arg()]}),
    case PrefetchCount of
        none -> ok;
        _    -> amqp_channel:call(Ch,
                                  #'basic.qos'{prefetch_count = PrefetchCount})
    end,
    #'basic.consume_ok'{} =
        amqp_channel:subscribe(Ch, #'basic.consume'{queue  = Q,
                                                    no_ack = false}, self()),
    State#state{queue = Q}.

ensure_upstream_bindings(State = #state{upstream            = Upstream,
                                        connection          = Conn,
                                        channel             = Ch,
                                        downstream_exchange = DownstreamX,
                                        queue               = Q}) ->
    #upstream{exchange = X,
              params   = #amqp_params_network{virtual_host = VHost}} = Upstream,
    OldSuffix = rabbit_federation_db:get_active_suffix(DownstreamX, Upstream),
    Suffix = case OldSuffix of
                 <<"A">> -> <<"B">>;
                 <<"B">> -> <<"A">>
             end,
    InternalX = upstream_exchange_name(X, VHost, DownstreamX, Suffix),
    delete_upstream_exchange(Conn, InternalX),
    amqp_channel:call(
      Ch, #'exchange.declare'{
        exchange    = InternalX,
        type        = <<"fanout">>,
        durable     = true,
        internal    = true,
        auto_delete = true,
        arguments   = [rabbit_federation_util:purpose_arg()]}),
    amqp_channel:call(Ch, #'queue.bind'{exchange = InternalX, queue = Q}),
    State1 = State#state{queue             = Q,
                         internal_exchange = InternalX},
    State2 = lists:foldl(fun (B, State0) ->
                                 add_binding(B, State0)
                         end,
                         State1, rabbit_binding:list_for_source(DownstreamX)),
    rabbit_federation_db:set_active_suffix(DownstreamX, Upstream, Suffix),
    OldInternalX = upstream_exchange_name(X, VHost, DownstreamX, OldSuffix),
    delete_upstream_exchange(Conn, OldInternalX),
    State2.

upstream_queue_name(X, VHost, #resource{name         = DownstreamName,
                                        virtual_host = DownstreamVHost}) ->
    Node = list_to_binary(atom_to_list(node())),
    DownstreamPart = case DownstreamVHost of
                         VHost -> case DownstreamName of
                                      X -> <<"">>;
                                      _ -> <<":", DownstreamName/binary>>
                                  end;
                         _     -> <<":", DownstreamVHost/binary,
                                    ":", DownstreamName/binary>>
                     end,
    <<X/binary, " -> ", Node/binary, DownstreamPart/binary>>.

upstream_exchange_name(X, VHost, DownstreamX, Suffix) ->
    Name = upstream_queue_name(X, VHost, DownstreamX),
    <<Name/binary, " ", Suffix/binary>>.

delete_upstream_queue(Conn, Q) ->
    disposable_channel_call(Conn, #'queue.delete'{queue = Q}).

delete_upstream_exchange(Conn, X) ->
    disposable_channel_call(Conn, #'exchange.delete'{exchange = X}).

disposable_channel_call(Conn, Method) ->
    {ok, Ch} = amqp_connection:open_channel(Conn),
    try
        amqp_channel:call(Ch, Method)
    catch exit:{{shutdown, {server_initiated_close, _, _}}, _} ->
            ok
    end,
    ensure_closed(Ch).

ensure_closed(Conn, Ch) ->
    ensure_closed(Ch),
    catch amqp_connection:close(Conn).

ensure_closed(Ch) ->
    catch amqp_channel:close(Ch).

%% For the time being just don't forward anything that's already been
%% forwarded.
forwarded_before(undefined) ->
    false;
forwarded_before(Headers) ->
    rabbit_misc:table_lookup(Headers, ?ROUTING_HEADER) =/= undefined.

extract_headers(#amqp_msg{props = #'P_basic'{headers = Headers}}) ->
    Headers.

update_headers(Headers, Msg = #amqp_msg{props = Props}) ->
    Msg#amqp_msg{props = Props#'P_basic'{headers = Headers}}.

add_routing_to_headers(undefined, Info) ->
    add_routing_to_headers([], Info);
add_routing_to_headers(Headers, Info) ->
    Prior = case rabbit_misc:table_lookup(Headers, ?ROUTING_HEADER) of
                undefined          -> [];
                {array, Existing}  -> Existing
            end,
    set_table_value(Headers, ?ROUTING_HEADER, array, [{table, Info}|Prior]).

upstream_info(#upstream{params   = #amqp_params_network{host         = H,
                                                        port         = P,
                                                        virtual_host = V,
                                                        ssl_options  = SSL},
                        exchange = X}) ->
    Protocol = case SSL of
                   none -> <<"amqp">>;
                   _    -> <<"amqps">>
               end,
    [{<<"host">>,         longstr, list_to_binary(H)},
     {<<"protocol">>,     longstr, Protocol},
     {<<"port">>,         long,    P},
     {<<"virtual_host">>, longstr, V},
     {<<"exchange">>,     longstr, X}].

%% TODO move this to rabbit_misc?
set_table_value(Table, Key, Type, Value) ->
    Stripped =
      case rabbit_misc:table_lookup(Table, Key) of
          {Type, _}  -> {_, Rest} = lists:partition(fun ({K, _, _}) ->
                                                            K == Key
                                                    end, Table),
                        Rest;
          {Type2, _} -> exit({type_mismatch_updating_table, Type, Type2});
          undefined  -> Table
      end,
    rabbit_misc:sort_field_table([{Key, Type, Value}|Stripped]).
