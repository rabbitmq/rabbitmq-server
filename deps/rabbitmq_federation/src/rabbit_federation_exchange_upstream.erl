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
-include("rabbit_federation.hrl").

-behaviour(gen_server2).

-export([start_link/1]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-export([add_routing_to_headers/2]).

-define(ROUTING_HEADER, <<"x-forwarding">>).

-record(state, {vhost,
                connection,
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
    {ok, DConn} = amqp_connection:start(direct,
                                        rabbit_federation_util:local_params()),
    {ok, DCh} = amqp_connection:open_channel(DConn),
    #'confirm.select_ok'{} = amqp_channel:call(DCh, #'confirm.select'{}),
    amqp_channel:register_confirm_handler(DCh, self()),
    erlang:monitor(process, DCh),
    Props = rabbit_federation_util:parse_uri(URI),
    X = proplists:get_value(exchange, Props),
    VHost = proplists:get_value(vhost, Props),
    Params = rabbit_federation_util:params_from_uri(URI),
    {ok, Conn} = amqp_connection:start(network, Params#params.connection),
    {ok, Ch} = amqp_connection:open_channel(Conn),
    erlang:monitor(process, Ch),
    State = #state{downstream_connection = DConn,
                   downstream_channel    = DCh,
                   downstream_exchange   = DownstreamX,
                   downstream_durable    = Durable,
                   uri                   = URI,
                   vhost                 = VHost,
                   connection            = Conn,
                   channel               = Ch,
                   exchange              = X,
                   unacked               = gb_trees:empty()},
    State1 = consume_from_upstream_queue(Params, State),
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

consume_from_upstream_queue(#params{prefetch_count = PrefetchCount,
                                    queue_expires  = Expiry},
                            State = #state{connection          = Conn,
                                           channel             = Ch,
                                           exchange            = X,
                                           vhost               = VHost,
                                           downstream_exchange = DownstreamX,
                                           downstream_durable  = Durable}) ->
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
    amqp_channel:call(Ch, #'basic.qos'{prefetch_count = PrefetchCount}),
    #'basic.consume_ok'{} =
        amqp_channel:subscribe(Ch, #'basic.consume'{queue  = Q,
                                                    no_ack = false}, self()),
    State#state{queue = Q}.

ensure_upstream_bindings(State = #state{vhost               = VHost,
                                        connection          = Conn,
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
    State1 = State#state{queue = Q, internal_exchange = InternalX},
    [add_binding(B, State1) ||
        B <- rabbit_binding:list_for_source(DownstreamX)],
    rabbit_federation_db:set_active_suffix(DownstreamX, URI, Suffix),
    OldInternalX = upstream_exchange_name(X, VHost, DownstreamX, OldSuffix),
    delete_upstream_exchange(Conn, OldInternalX),
    State1.

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
    <<X/binary, " ⇨ ", Node/binary, DownstreamPart/binary>>.

upstream_exchange_name(X, VHost, DownstreamX, Suffix) ->
    Name = upstream_queue_name(X, VHost, DownstreamX),
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
