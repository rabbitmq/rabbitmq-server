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

-import(rabbit_misc, [pget/2]).

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
                downstream_exchange,
                unacked = gb_trees:empty()}).

%%----------------------------------------------------------------------------

%% We start off in a state where we do not connect, since we can first
%% start during exchange recovery, when rabbit is not fully started
%% and the Erlang client is not running. This then gets invoked when
%% the federation app is started.
go() -> cast(go).

add_binding(S, XN, B)      -> cast(XN, {enqueue, S, {add_binding, B}}).
remove_bindings(S, XN, Bs) -> cast(XN, {enqueue, S, {remove_bindings, Bs}}).

%% This doesn't just correspond to the link being killed by its
%% supervisor since this means "the exchange is going away, please
%% remove upstream resources associated with it".
stop(XN) -> call(XN, stop).

list_routing_keys(XN) -> call(XN, list_routing_keys).

%%----------------------------------------------------------------------------

start_link(Args) ->
    report_status(Args, starting),
    gen_server2:start_link(?MODULE, Args, [{timeout, infinity}]).

init(Args = {_, XName}) ->
    join(rabbit_federation_exchanges),
    join({rabbit_federation_exchange, XName}),
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
    case federation_up() of
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

handle_info(#'basic.ack'{delivery_tag = Seq, multiple = Multiple},
            State = #state{unacked = Unacked}) ->
    ack(gb_trees:get(Seq, Unacked), Multiple, State),
    {noreply, State#state{unacked = remove_delivery_tags(Seq, Multiple,
                                                         Unacked)}};

handle_info({#'basic.deliver'{routing_key  = Key,
                              delivery_tag = DTag,
                              redelivered  = Redelivered}, Msg},
            State = #state{
              upstream            = #upstream{max_hops = MaxHops} = Upstream,
              downstream_exchange = #resource{name = XNameBin},
              downstream_channel  = DCh,
              unacked             = Unacked}) ->
    Headers0 = extract_headers(Msg),
    %% TODO add user information here?
    case should_forward(Headers0, MaxHops) of
        true  -> {table, Info0} = rabbit_federation_upstream:to_table(Upstream),
                 Info = Info0 ++ [{<<"redelivered">>, bool, Redelivered}],
                 Headers = add_routing_to_headers(Headers0, Info),
                 Seq = amqp_channel:next_publish_seqno(DCh),
                 amqp_channel:cast(DCh, #'basic.publish'{exchange    = XNameBin,
                                                         routing_key = Key},
                                   update_headers(Headers, Msg)),
                 {noreply, State#state{unacked = gb_trees:insert(Seq, DTag,
                                                                 Unacked)}};
        false -> ack(DTag, false, State), %% Drop it, but acknowledge it!
                 {noreply, State}
    end;

%% If the downstream channel shuts down cleanly, we can just ignore it
%% - we're the same node, we're presumably about to go down too.
handle_info({'DOWN', _Ref, process, Ch, shutdown},
            State = #state{downstream_channel = Ch}) ->
    {noreply, State};

%% If the upstream channel goes down for an intelligible reason, just
%% log it and die quietly.
handle_info({'DOWN', _Ref, process, Ch, {shutdown, Reason}},
            State = #state{channel = Ch}) ->
    connection_error({upstream_channel_down, Reason}, State);

handle_info({'DOWN', _Ref, process, Ch, Reason},
            State = #state{channel = Ch}) ->
    {stop, {upstream_channel_down, Reason}, State};

handle_info({'DOWN', _Ref, process, Ch, Reason},
            State = #state{downstream_channel = Ch}) ->
    {stop, {downstream_channel_down, Reason}, State};

handle_info(Msg, State) ->
    {stop, {unexpected_info, Msg}, State}.

terminate(Reason, State = {not_started, _}) ->
    report_status(State, map_error(Reason)),
    ok;

terminate(Reason, State = #state{downstream_channel    = DCh,
                                 downstream_connection = DConn,
                                 connection            = Conn,
                                 channel               = Ch}) ->
    report_status(State, map_error(Reason)),
    ensure_closed(DConn, DCh),
    ensure_closed(Conn, Ch),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%----------------------------------------------------------------------------

call(XName, Msg) -> [gen_server2:call(Pid, Msg, infinity) || Pid <- x(XName)].
cast(Msg)        -> [gen_server2:cast(Pid, Msg) || Pid <- all()].
cast(XName, Msg) -> [gen_server2:cast(Pid, Msg) || Pid <- x(XName)].

join(Name) ->
    pg2_fixed:create(Name),
    ok = pg2_fixed:join(Name, self()).

all() ->
    pg2_fixed:create(rabbit_federation_exchanges),
    pg2_fixed:get_members(rabbit_federation_exchanges).

x(XName) ->
    pg2_fixed:create({rabbit_federation_exchange, XName}),
    pg2_fixed:get_members({rabbit_federation_exchange, XName}).

%%----------------------------------------------------------------------------

federation_up() ->
    lists:keysearch(rabbitmq_federation, 1,
                    application:which_applications(infinity)) =/= false.

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
            State = #state{channel = Ch, internal_exchange = IntXNameBin,
                           upstream = #upstream{exchange = XNameBin}}) ->
    case check_add_binding(B, State) of
        {true,  State1} -> State1;
        {false, State1} -> amqp_channel:call(
                             Ch, #'exchange.bind'{
                               destination = IntXNameBin,
                               source      = XNameBin,
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
               State = #state{channel = Ch,
                              internal_exchange = IntXNameBin,
                              upstream = #upstream{exchange = XNameBin}}) ->
    case check_remove_binding(B, State) of
        {true,  State1} -> State1;
        {false, State1} -> amqp_channel:call(
                             Ch, #'exchange.unbind'{
                               destination = IntXNameBin,
                               source      = XNameBin,
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

go(S0 = {not_started, {Upstream, DownXName =
                           #resource{virtual_host = DownVHost}}}) ->
    case open(rabbit_federation_util:local_params(DownVHost)) of
        {ok, DConn, DCh} ->
            #'confirm.select_ok'{} =
               amqp_channel:call(DCh, #'confirm.select'{}),
            amqp_channel:register_confirm_handler(DCh, self()),
            case open(Upstream#upstream.params) of
                {ok, Conn, Ch} ->
                    {Serial, Bindings} =
                        rabbit_misc:execute_mnesia_transaction(
                          fun () ->
                                  {rabbit_exchange:peek_serial(DownXName),
                                   rabbit_binding:list_for_source(DownXName)}
                          end),
                    %% If we are very short lived, Serial can be undefined at
                    %% this point (since the deletion of the X could have
                    %% overtaken the creation of this process). However, this
                    %% is not a big deal - 'undefined' just becomes the next
                    %% serial we will process. Since it compares larger than
                    %% any number we never process any commands. And we will
                    %% soon get told to stop anyway.
                    State = ensure_upstream_bindings(
                              consume_from_upstream_queue(
                                #state{upstream              = Upstream,
                                       connection            = Conn,
                                       channel               = Ch,
                                       next_serial           = Serial,
                                       downstream_connection = DConn,
                                       downstream_channel    = DCh,
                                       downstream_exchange   = DownXName}),
                              Bindings),
                    rabbit_log:info("Federation ~s connected to ~s~n",
                                    [rabbit_misc:rs(DownXName),
                                     rabbit_federation_upstream:to_string(
                                       Upstream)]),
                    Name = pget(name, amqp_connection:info(DConn, [name])),
                    report_status(State, {connected, Name}),
                    {noreply, State};
                E ->
                    ensure_closed(DConn, DCh),
                    connection_error(E, S0)
            end;
        E ->
            connection_error(E, S0)
    end.

connection_error(E, State = {not_started, {U, XName}}) ->
    rabbit_log:info("Federation ~s failed to establish connection to ~s~n~p~n",
                    [rabbit_misc:rs(XName),
                     rabbit_federation_upstream:to_string(U), E]),
    {stop, {shutdown, {connect_failed, E}}, State};

connection_error(E, State = #state{upstream            = U,
                                   downstream_exchange = XName}) ->
    rabbit_log:info("Federation ~s disconnected from ~s:~n~p~n",
                    [rabbit_misc:rs(XName),
                     rabbit_federation_upstream:to_string(U), E]),
    {stop, {shutdown, {disconnected, E}}, State}.


consume_from_upstream_queue(
  State = #state{upstream            = Upstream,
                 channel             = Ch,
                 downstream_exchange = DownXName}) ->
    #upstream{exchange       = XNameBin,
              prefetch_count = Prefetch,
              expires        = Expiry,
              message_ttl    = TTL,
              params         = #amqp_params_network{virtual_host = VHost}}
        = Upstream,
    Q = upstream_queue_name(XNameBin, VHost, DownXName),
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
                                        downstream_exchange = DownXName,
                                        queue               = Q}, Bindings) ->
    #upstream{exchange = XNameBin,
              params   = #amqp_params_network{virtual_host = VHost}} = Upstream,
    OldSuffix = rabbit_federation_db:get_active_suffix(
                  DownXName, Upstream, <<"A">>),
    Suffix = case OldSuffix of
                 <<"A">> -> <<"B">>;
                 <<"B">> -> <<"A">>
             end,
    IntXNameBin = upstream_exchange_name(XNameBin, VHost, DownXName, Suffix),
    delete_upstream_exchange(Conn, IntXNameBin),
    amqp_channel:call(
      Ch, #'exchange.declare'{
        exchange    = IntXNameBin,
        type        = <<"fanout">>,
        durable     = true,
        internal    = true,
        auto_delete = true,
        arguments   = []}),
    amqp_channel:call(Ch, #'queue.bind'{exchange = IntXNameBin, queue = Q}),
    State1 = State#state{internal_exchange = IntXNameBin},
    State2 = lists:foldl(fun add_binding/2, State1, Bindings),
    rabbit_federation_db:set_active_suffix(DownXName, Upstream, Suffix),
    OldIntXNameBin = upstream_exchange_name(
                       XNameBin, VHost, DownXName, OldSuffix),
    delete_upstream_exchange(Conn, OldIntXNameBin),
    State2.

upstream_queue_name(XNameBin, VHost, #resource{name         = DownXNameBin,
                                               virtual_host = DownVHost}) ->
    Node = local_nodename(),
    DownPart = case DownVHost of
                   VHost -> case DownXNameBin of
                                XNameBin -> <<"">>;
                                _        -> <<":", DownXNameBin/binary>>
                            end;
                   _     -> <<":", DownVHost/binary,
                              ":", DownXNameBin/binary>>
               end,
    <<"federation: ", XNameBin/binary, " -> ", Node/binary, DownPart/binary>>.

upstream_exchange_name(XNameBin, VHost, DownXName, Suffix) ->
    Name = upstream_queue_name(XNameBin, VHost, DownXName),
    <<Name/binary, " ", Suffix/binary>>.

local_nodename() ->
    {ok, Explicit} = application:get_env(rabbitmq_federation, local_nodename),
    case Explicit of
        automatic -> {ID, _} = rabbit_misc:nodeparts(node()),
                     {ok, Host} = inet:gethostname(),
                     {ok, #hostent{h_name = FQDN}} = inet:gethostbyname(Host),
                     list_to_binary(atom_to_list(
                                      rabbit_misc:makenode({ID, FQDN})));
        _         -> list_to_binary(Explicit)
    end.

delete_upstream_exchange(Conn, XNameBin) ->
    disposable_channel_call(Conn, #'exchange.delete'{exchange = XNameBin}).

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

ack(Tag, Multiple, #state{channel = Ch}) ->
    amqp_channel:cast(Ch, #'basic.ack'{delivery_tag = Tag,
                                       multiple     = Multiple}).

remove_delivery_tags(Seq, false, Unacked) ->
    gb_trees:delete(Seq, Unacked);
remove_delivery_tags(Seq, true, Unacked) ->
    case gb_trees:is_empty(Unacked) of
        true  -> Unacked;
        false -> {Smallest, _Val, Unacked1} = gb_trees:take_smallest(Unacked),
                 case Smallest > Seq of
                     true  -> Unacked;
                     false -> remove_delivery_tags(Seq, true, Unacked1)
                 end
    end.

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

report_status({#upstream{connection_name = Connection,
                         exchange        = UXNameBin}, XName}, Status) ->
    rabbit_federation_status:report(XName, Connection, UXNameBin, Status);

report_status({not_started, Args}, Status) ->
    report_status(Args, Status);

report_status(#state{upstream            = Upstream,
                     downstream_exchange = XName}, Status) ->
    report_status({Upstream, XName}, Status).

map_error({shutdown, {connect_failed, {error, E}}}) -> {connect_failed, E};
map_error({shutdown, Reason})                       -> Reason;
map_error(Term)                                     -> Term.
