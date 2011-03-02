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

-module(rabbit_federation_exchange_upstream).

-include_lib("amqp_client/include/amqp_client.hrl").

-behaviour(gen_server2).

-export([start_link/1]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-export([add_routing_to_headers/2]).

-define(ROUTING_HEADER, <<"x-forwarding">>).

-record(state, {connection,
                channel,
                queue,
                exchange,
                internal_exchange,
                uri,
                unacked,
                downstream_connection,
                downstream_channel,
                downstream_exchange,
                downstream_durable,
                next_publish_id = 1}).

%%----------------------------------------------------------------------------

start_link({URI, DownstreamX, Durable}) ->
    gen_server2:start_link(?MODULE, {URI, DownstreamX, Durable},
                           [{timeout, infinity}]).

%%----------------------------------------------------------------------------

init(Args) ->
    gen_server2:cast(self(), {init, Args}),
    {ok, not_started}.

handle_call({add_binding, Binding}, _From, State) ->
    add_binding(Binding, State),
    {reply, ok, State};

handle_call({remove_binding, #binding{key = Key, args = Args}}, _From,
            State = #state{connection = Conn, queue = Q, exchange = X}) ->
    %% We may already be unbound if e.g. someone has deleted the upstream
    %% exchange
    with_disposable_channel(
      Conn,
      fun (Ch) ->
              amqp_channel:call(Ch, #'exchange.unbind'{destination = Q,
                                                       source      = X,
                                                       routing_key = Key,
                                                       arguments   = Args})
      end),
    {reply, ok, State};

handle_call(stop, _From, State = #state{connection = Conn, queue = Q}) ->
    with_disposable_channel(
      Conn, fun (Ch) -> amqp_channel:call(Ch, #'queue.delete'{queue = Q}) end),
    {stop, normal, ok, State};

handle_call(Msg, _From, State) ->
    {stop, {unexpected_call, Msg}, State}.

handle_cast({init, {URI, DownstreamX, Durable}}, not_started) ->
    {ok, DConn} = amqp_connection:start(direct),
    {ok, DCh} = amqp_connection:open_channel(DConn),
    #'confirm.select_ok'{} = amqp_channel:call(DCh, #'confirm.select'{}),
    amqp_channel:register_confirm_handler(DCh, self()),
    erlang:monitor(process, DCh),
    X = proplists:get_value(exchange, rabbit_federation_util:parse_uri(URI)),
    Params = params_from_uri(URI),
    {ok, Conn} = amqp_connection:start(network, Params),
    {ok, Ch} = amqp_connection:open_channel(Conn),
    erlang:monitor(process, Ch),
    State = #state{downstream_connection = DConn,
                   downstream_channel    = DCh,
                   downstream_exchange   = DownstreamX,
                   downstream_durable    = Durable,
                   uri                   = URI,
                   connection            = Conn,
                   channel               = Ch,
                   exchange              = X,
                   unacked               = gb_trees:empty()},
    State1 = consume_from_upstream_queue(State),
    State2 = ensure_upstream_bindings(State1),
    {noreply, State2};

handle_cast(Msg, State) ->
    {stop, {unexpected_cast, Msg}, State}.

handle_info(#'basic.consume_ok'{}, State) ->
    {noreply, State};

handle_info(#'basic.ack'{ delivery_tag = Seq, multiple = Multiple },
            State = #state{channel = Ch}) ->
    {DTag, State1} = retrieve_delivery_tag(Seq, Multiple, State),
    case DTag of
        none -> ok;
        _    -> amqp_channel:cast(Ch, #'basic.ack'{delivery_tag = DTag,
                                                   multiple     = Multiple})
    end,
    {noreply, State1};

handle_info({#'basic.deliver'{delivery_tag = DTag,
                              %% TODO do we care?
                              %%redelivered = Redelivered,
                              %%exchange = Exchange,
                              routing_key = Key}, Msg},
            State = #state{uri                 = URI,
                           downstream_exchange = #resource{name = X},
                           downstream_channel  = DCh,
                           next_publish_id     = Seq}) ->
    Headers0 = extract_headers(Msg),
    case forwarded_before(Headers0) of
        false -> State1 = record_delivery_tag(DTag, Seq, State),
                 %% TODO add user information here?
                 Headers = add_routing_to_headers(Headers0,
                                                  [{<<"uri">>, longstr, URI}]),
                 amqp_channel:cast(DCh, #'basic.publish'{exchange    = X,
                                                         routing_key = Key},
                                   update_headers(Headers, Msg)),
                 {noreply, State1#state{next_publish_id = Seq + 1}};
        true  -> {noreply, State}
    end;

handle_info({'DOWN', _Ref, process, Ch, Reason},
            State = #state{ downstream_channel = DCh }) ->
    case Ch of
        DCh -> {stop, {downstream_channel_down, Reason}, State};
        _   -> {stop, {upstream_channel_down, Reason}, State}
    end;

handle_info(Msg, State) ->
    {stop, {unexpected_info, Msg}, State}.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

terminate(_Reason, #state{downstream_channel    = DCh,
                          downstream_connection = DConn,
                          connection            = Conn,
                          channel               = Ch}) ->
    ensure_closed(DConn, DCh),
    ensure_closed(Conn, Ch),
    ok.

%%----------------------------------------------------------------------------

add_binding(#binding{key = Key, args = Args},
            #state{channel = Ch, exchange = X,
                   internal_exchange = InternalX}) ->
    amqp_channel:call(Ch, #'exchange.bind'{destination = InternalX,
                                           source      = X,
                                           routing_key = Key,
                                           arguments   = Args}).

%%----------------------------------------------------------------------------

consume_from_upstream_queue(State = #state{connection          = Conn,
                                           channel             = Ch,
                                           exchange            = X,
                                           downstream_exchange = DownstreamX,
                                           downstream_durable  = Durable}) ->
    Q = upstream_queue_name(X, DownstreamX),
    case Durable of
        false -> delete_upstream_queue(Conn, Q);
        _     -> ok
    end,
    amqp_channel:call(
      Ch, #'queue.declare'{
        queue     = Q,
        durable   = true,
        %% TODO: The x-expires should be configurable.
        arguments = [{<<"x-expires">>, long, 86400000}] }),
    #'basic.consume_ok'{} =
        amqp_channel:subscribe(Ch, #'basic.consume'{queue  = Q,
                                                    no_ack = false}, self()),
    State#state{queue = Q}.

ensure_upstream_bindings(State = #state{connection          = Conn,
                                        channel             = Ch,
                                        uri                 = URI,
                                        exchange            = X,
                                        downstream_exchange = DownstreamX,
                                        queue               = Q}) ->
    OldSuffix = rabbit_federation_db:get_active_suffix(DownstreamX, URI),
    Suffix = case OldSuffix of
                 <<"A">> -> <<"B">>;
                 <<"B">> -> <<"A">>
             end,
    InternalX = upstream_exchange_name(X, DownstreamX, Suffix),
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
    State1 = State#state{queue = Q, internal_exchange = InternalX},
    [add_binding(B, State1) ||
        B <- rabbit_binding:list_for_source(DownstreamX)],
    rabbit_federation_db:set_active_suffix(DownstreamX, URI, Suffix),
    OldInternalX = upstream_exchange_name(X, DownstreamX, OldSuffix),
    delete_upstream_exchange(Conn, OldInternalX),
    State1.

upstream_queue_name(X, #resource{name         = DownstreamName,
                                 virtual_host = DownstreamVHost}) ->
    Node = list_to_binary(atom_to_list(node())),
    <<"federation: ", X/binary, " -> ", Node/binary,
      "-", DownstreamVHost/binary, "-", DownstreamName/binary>>.

upstream_exchange_name(X, DownstreamX, Suffix) ->
    Name = upstream_queue_name(X, DownstreamX),
    <<Name/binary, " ", Suffix/binary>>.

delete_upstream_queue(Conn, Q) ->
    with_disposable_channel(
      Conn, fun (Ch) -> amqp_channel:call(Ch, #'queue.delete'{queue = Q}) end).

delete_upstream_exchange(Conn, X) ->
    with_disposable_channel(
      Conn, fun (Ch) ->
                    amqp_channel:call(Ch, #'exchange.delete'{exchange = X})
            end).

%%----------------------------------------------------------------------------

record_delivery_tag(DTag, Seq, State = #state{unacked = Unacked}) ->
    State#state{unacked = gb_trees:insert(Seq, DTag, Unacked)}.

retrieve_delivery_tag(Seq, Multiple,
                      State = #state {unacked = Unacked0}) ->
    Unacked = remove_delivery_tags(Seq, Multiple, Unacked0),
    DTag = case gb_trees:lookup(Seq, Unacked0) of
               {value, V} -> V;
               none       -> none
           end,
    {DTag, State#state{unacked = Unacked}}.

remove_delivery_tags(Seq, false, Unacked) ->
    gb_trees:delete_any(Seq, Unacked);
remove_delivery_tags(Seq, true, Unacked) ->
    case gb_trees:size(Unacked) of
        0 -> Unacked;
        _ -> Smallest = gb_trees:smallest(Unacked),
             case Smallest > Seq of
                 true  -> Unacked;
                 false -> remove_delivery_tags(
                            Seq, true, gb_trees:delete(Smallest, Unacked))
             end
    end.

%%----------------------------------------------------------------------------

with_disposable_channel(Conn, Fun) ->
    {ok, Ch} = amqp_connection:open_channel(Conn),
    try
        Fun(Ch)
    catch exit:{{server_initiated_close, _, _}, _} ->
            ok
    end,
    ensure_closed(Ch).

ensure_closed(Conn, Ch) ->
    ensure_closed(Ch),
    catch amqp_connection:close(Conn).

ensure_closed(Ch) ->
    catch amqp_channel:close(Ch).

%%----------------------------------------------------------------------------

params_from_uri(ExchangeURI) ->
    Props = rabbit_federation_util:parse_uri(ExchangeURI),
    Params = #amqp_params{host         = proplists:get_value(host, Props),
                          port         = proplists:get_value(port, Props),
                          virtual_host = proplists:get_value(vhost, Props)},
    {ok, Brokers} = application:get_env(rabbit_federation, brokers),
    Usable = [Merged || Broker <- Brokers,
                        Merged <- [merge(Broker, Props)],
                        all_match(Broker, Merged)],
    case Usable of
        []    -> Params;
        [B|_] -> params_from_broker(Params, B)
    end.

params_from_broker(P, B) ->
    P1 = P#amqp_params{
           username = list_to_binary(proplists:get_value(username, B, "guest")),
           password = list_to_binary(proplists:get_value(password, B, "guest"))
          },
    P2 = case proplists:get_value(scheme, B, "amqp") of
             "amqp"  -> P1;
             "amqps" -> {ok, Opts} = application:get_env(
                                       rabbit_federation, ssl_options),
                        P1#amqp_params{ssl_options = Opts,
                                       port        = 5671}
         end,
    case proplists:get_value(mechanism, B, 'PLAIN') of
        'PLAIN'    -> P2;
        %% TODO it would be nice to support arbitrary mechanisms here.
        'EXTERNAL' -> P2#amqp_params{auth_mechanisms =
                                         [fun amqp_auth_mechanisms:external/3]};
        M          -> exit({unsupported_mechanism, M})
    end.

%% For all the props in Props1, does Props2 match?
all_match(Props1, Props2) ->
    lists:all(fun ({K, V}) ->
                      proplists:get_value(K, Props2) == V
              end, [KV || KV <- Props1]).

%% Add elements of Props1 which are not in Props2 - i.e. Props2 wins in event
%% of a clash
merge(Props1, Props2) ->
    lists:foldl(fun({K, V}, P) ->
                        case proplists:is_defined(K, Props2) of
                            true  -> P;
                            false -> [{K, V}|P]
                        end
                end, Props2, Props1).

%%----------------------------------------------------------------------------

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
