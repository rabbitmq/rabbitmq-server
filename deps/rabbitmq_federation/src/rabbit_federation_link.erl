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

-include_lib("kernel/include/inet.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").
-include("rabbit_federation.hrl").

-behaviour(gen_server2).

-export([go/0, add_binding/3, remove_bindings/3, stop/1]).
-export([list_routing_keys/1]). %% For testing

-export([start_link/1]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-export([add_routing_to_headers/2]).

-define(ROUTING_HEADER, <<"x-received-from">>).

-record(state, {upstream,
                connection,
                channel,
                queue,
                internal_exchange,
                waiting_cmds = gb_trees:empty(),
                next_serial,
                bindings = dict:new(),
                downstream_connection,
                downstream_channel,
                downstream_exchange}).

%%----------------------------------------------------------------------------

%% We start off in a state where we do not connect, since we can first
%% start during exchange recovery, when rabbit is not fully started
%% and the Erlang client is not running. This then gets invoked when
%% the federation app is started.
go() -> cast(go).

add_binding(S, X, B)      -> cast(X, {enqueue, S, {add_binding, B}}).
remove_bindings(S, X, Bs) -> cast(X, {enqueue, S, {remove_bindings, Bs}}).

%% This doesn't just correspond to the link being killed by its
%% supervisor since this means "the exchange is going away, please
%% remove upstream resources associated with it".
stop(X) -> call(X, stop).

list_routing_keys(X) -> call(X, list_routing_keys).

%%----------------------------------------------------------------------------

start_link(Args) ->
    gen_server2:start_link(?MODULE, Args, [{timeout, infinity}]).

init(Args = {_, X}) ->
    join(rabbit_federation_exchanges),
    join({rabbit_federation_exchange, X}),
    gen_server2:cast(self(), maybe_go),
    {ok, {not_started, Args}}.

handle_call(list_routing_keys, _From, State = #state{bindings = Bindings}) ->
    {reply, lists:sort([K || {K, _} <- dict:fetch_keys(Bindings)]), State};

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
%% before 'go' gets invoked. Ignore.
handle_cast(go, State) ->
    {noreply, State};

handle_cast({enqueue, _, _}, State = {not_started, _}) ->
    {noreply, State};

handle_cast({enqueue, Serial, Cmd}, State = #state{waiting_cmds = Waiting}) ->
    Waiting1 = gb_trees:insert(Serial, Cmd, Waiting),
    {noreply, play_back_commands(State#state{waiting_cmds = Waiting1})};

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

handle_info({#'basic.deliver'{routing_key  = Key,
                              delivery_tag = Seq}, Msg},
            State = #state{
              upstream            = #upstream{max_hops = MaxHops} = Upstream,
              channel             = Ch,
              downstream_exchange = #resource{name = X},
              downstream_channel  = DCh}) ->
    Headers0 = extract_headers(Msg),
    %% TODO add user information here?
    case should_forward(Headers0, MaxHops) of
        true  -> {table, Info} = rabbit_federation_upstream:to_table(Upstream),
                 Headers = add_routing_to_headers(Headers0, Info),
                 amqp_channel:cast(DCh, #'basic.publish'{exchange    = X,
                                                         routing_key = Key},
                                   update_headers(Headers, Msg)),
                 ok;
        false -> %% Drop it, but acknowledge it!
                 amqp_channel:cast(Ch, #'basic.ack'{delivery_tag = Seq,
                                                    multiple     = false}),
                 ok
    end,
    {noreply, State};

handle_info({'DOWN', _Ref, process, Ch, shutdown},
            State = #state{ downstream_channel = Ch }) ->
    {noreply, State};

handle_info({'DOWN', _Ref, process, Ch, Reason},
            State = #state{ channel = Ch }) ->
    {stop, {upstream_channel_down, Reason}, State};
handle_info({'DOWN', _Ref, process, Ch, Reason},
            State = #state{ downstream_channel = Ch }) ->
    {stop, {downstream_channel_down, Reason}, State};

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

call(X, Msg) -> [gen_server2:call(Pid, Msg, infinity) || Pid <- x(X)].
cast(Msg) ->    [gen_server2:cast(Pid, Msg) || Pid <- all()].
cast(X, Msg) -> [gen_server2:cast(Pid, Msg) || Pid <- x(X)].

join(Name) ->
    pg2_fixed:create(Name),
    ok = pg2_fixed:join(Name, self()).

all() ->
    pg2_fixed:create(rabbit_federation_exchanges),
    pg2_fixed:get_members(rabbit_federation_exchanges).

x(X) ->
    pg2_fixed:create({rabbit_federation_exchange, X}),
    pg2_fixed:get_members({rabbit_federation_exchange, X}).

%%----------------------------------------------------------------------------

handle_command({add_binding, Binding}, State) ->
    add_binding(Binding, State);

handle_command({remove_bindings, Bindings}, State) ->
    lists:foldl(fun remove_binding/2, State, Bindings).

play_back_commands(State = #state{waiting_cmds = Waiting,
                                  next_serial  = Next}) ->
    case gb_trees:is_empty(Waiting) of
        false -> case gb_trees:take_smallest(Waiting) of
                     {Next, Cmd, Waiting1} ->
                         %% The next one. Just execute it.
                         play_back_commands(
                           handle_command(Cmd, State#state{
                                                 waiting_cmds = Waiting1,
                                                 next_serial  = Next + 1}));
                     {Serial, _Cmd, Waiting1} when Serial < Next ->
                         %% This command came from before we executed
                         %% binding:list_for_source. Ignore it.
                         play_back_commands(State#state{
                                              waiting_cmds = Waiting1});
                     _ ->
                         %% Some future command. Don't do anything.
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
        {true,  State1} -> State1;
        {false, State1} -> amqp_channel:call(
                             Ch, #'exchange.bind'{destination = InternalX,
                                                  source      = X,
                                                  routing_key = Key,
                                                  arguments   = Args}),
                           State1
    end.

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
        {true,  State1} -> State1;
        {false, State1} -> amqp_channel:call(
                             Ch, #'exchange.unbind'{destination = InternalX,
                                                    source      = X,
                                                    routing_key = Key,
                                                    arguments   = Args}),
                           State1
    end.

check_remove_binding(B = #binding{destination = Dest},
                     State = #state{bindings = Bs}) ->
    K = key(B),
    Dests = sets:del_element(Dest, dict:fetch(K, Bs)),
    case sets:size(Dests) of
        0 -> {false, State#state{bindings = dict:erase(K, Bs)}};
        _ -> {true,  State#state{bindings = dict:store(K, Dests, Bs)}}
    end.

key(#binding{key = Key, args = Args}) -> {Key, Args}.

go(S0 = {not_started, {Upstream, #exchange{name = DownstreamX}}}) ->
    case open(rabbit_federation_util:local_params()) of
        {ok, DConn, DCh} ->
            #'confirm.select_ok'{} =
               amqp_channel:call(DCh, #'confirm.select'{}),
            amqp_channel:register_confirm_handler(DCh, self()),
            case open(Upstream#upstream.params) of
                {ok, Conn, Ch} ->
                    {Serial, Bindings} =
                        rabbit_misc:execute_mnesia_transaction(
                          fun () ->
                                  {rabbit_exchange:peek_serial(DownstreamX),
                                   rabbit_binding:list_for_source(DownstreamX)}
                          end),
                    %% If we are very short lived, Serial can be undefined at
                    %% this point (since the deletion of the X could have
                    %% overtaken the creation of this process). However, this
                    %% is not a big deal - 'undefined' just becomes the next
                    %% serial we will process. Since it compares larger than
                    %% any number we never process any commands. And we will
                    %% soon get told to stop anyway.
                    {noreply,
                     ensure_upstream_bindings(
                       consume_from_upstream_queue(
                         #state{upstream              = Upstream,
                                connection            = Conn,
                                channel               = Ch,
                                next_serial           = Serial,
                                downstream_connection = DConn,
                                downstream_channel    = DCh,
                                downstream_exchange   = DownstreamX}),
                       Bindings)};
                E ->
                    ensure_closed(DConn, DCh),
                    connection_error(E, S0)
            end;
        E ->
            connection_error(E, S0)
    end.

connection_error(E, State = {not_started, {U, #exchange{name = X}}}) ->
    rabbit_log:info("Federation ~s failed to establish connection to ~s: ~w~n",
                    [rabbit_misc:rs(X),
                     rabbit_federation_upstream:to_string(U), E]),
    {stop, {shutdown, E}, State}.

consume_from_upstream_queue(
  State = #state{upstream            = Upstream,
                 channel             = Ch,
                 downstream_exchange = DownstreamX}) ->
    #upstream{exchange       = X,
              prefetch_count = Prefetch,
              expires        = Expiry,
              message_ttl    = TTL,
              params         = #amqp_params_network{virtual_host = VHost}}
        = Upstream,
    Q = upstream_queue_name(X, VHost, DownstreamX),
    ExpiryArg = case Expiry of
                    none -> [];
                    _    -> [{<<"x-expires">>, long, Expiry}]
                end,
    TTLArg = case TTL of
                 none -> [];
                 _    -> [{<<"x-message-ttl">>, long, TTL}]
             end,
    Args = ExpiryArg ++ TTLArg,
    amqp_channel:call(Ch, #'queue.declare'{queue     = Q,
                                           durable   = true,
                                           arguments = Args}),
    case Prefetch of
        none -> ok;
        _    -> amqp_channel:call(Ch, #'basic.qos'{prefetch_count = Prefetch})
    end,
    #'basic.consume_ok'{} =
        amqp_channel:subscribe(Ch, #'basic.consume'{queue  = Q,
                                                    no_ack = false}, self()),
    State#state{queue = Q}.

ensure_upstream_bindings(State = #state{upstream            = Upstream,
                                        connection          = Conn,
                                        channel             = Ch,
                                        downstream_exchange = DownstreamX,
                                        queue               = Q}, Bindings) ->
    #upstream{exchange = X,
              params   = #amqp_params_network{virtual_host = VHost}} = Upstream,
    OldSuffix = rabbit_federation_db:get_active_suffix(DownstreamX, Upstream,
                                                       <<"A">>),
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
        arguments   = []}),
    amqp_channel:call(Ch, #'queue.bind'{exchange = InternalX, queue = Q}),
    State1 = State#state{internal_exchange = InternalX},
    State2 = lists:foldl(fun add_binding/2, State1, Bindings),
    rabbit_federation_db:set_active_suffix(DownstreamX, Upstream, Suffix),
    OldInternalX = upstream_exchange_name(X, VHost, DownstreamX, OldSuffix),
    delete_upstream_exchange(Conn, OldInternalX),
    State2.

upstream_queue_name(X, VHost, #resource{name         = DownstreamName,
                                        virtual_host = DownstreamVHost}) ->
    Node = local_nodename(),
    DownstreamPart = case DownstreamVHost of
                         VHost -> case DownstreamName of
                                      X -> <<"">>;
                                      _ -> <<":", DownstreamName/binary>>
                                  end;
                         _     -> <<":", DownstreamVHost/binary,
                                    ":", DownstreamName/binary>>
                     end,
    <<"federation: ", X/binary, " -> ", Node/binary, DownstreamPart/binary>>.

upstream_exchange_name(X, VHost, DownstreamX, Suffix) ->
    Name = upstream_queue_name(X, VHost, DownstreamX),
    <<Name/binary, " ", Suffix/binary>>.

local_nodename() ->
    {ok, Explicit} = application:get_env(rabbitmq_federation, local_nodename),
    case Explicit of
        automatic -> [ID | _] = string:tokens(atom_to_list(node()), "@"),
                     {ok, Host} = inet:gethostname(),
                     {ok, #hostent{h_name = FQDN}} = inet:gethostbyname(Host),
                     list_to_binary(ID ++ "@" ++ FQDN);
        _         -> list_to_binary(Explicit)
    end.

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

should_forward(undefined, _MaxHops) ->
    true;
should_forward(Headers, MaxHops) ->
    case rabbit_misc:table_lookup(Headers, ?ROUTING_HEADER) of
        undefined  -> true;
        {array, A} -> length(A) < MaxHops
    end.

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
    rabbit_misc:set_table_value(
      Headers, ?ROUTING_HEADER, array, [{table, Info} | Prior]).
