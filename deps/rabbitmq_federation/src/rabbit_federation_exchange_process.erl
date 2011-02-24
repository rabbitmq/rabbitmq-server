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

-module(rabbit_federation_exchange_process).

-include_lib("amqp_client/include/amqp_client.hrl").
-include("rabbit_federation.hrl").

-behaviour(gen_server2).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-export([add_routing_to_headers/2]).

-record(upstream, { connection, channel, queue, exchange, uri, unacked,
                    consumer_tag }).

%%----------------------------------------------------------------------------

-define(ROUTING_HEADER, <<"x-forwarding">>).

%%----------------------------------------------------------------------------

init(_) ->
    exit(this_should_never_happen).

handle_call({add_binding, #binding{destination = Dest, key = Key, args = Args}},
            _From, State = #state{upstreams = Upstreams}) ->
    %% TODO add bindings only if needed.
    case is_federation_queue(Dest) of
        true  -> ok;
        false -> [bind_upstream(U, Key, Args) || U <- Upstreams]
    end,
    {reply, ok, State};

handle_call({remove_bindings, Bs }, _From,
            State = #state{ upstreams = Upstreams }) ->
    [maybe_unbind_upstreams(Upstreams, B) || B <- Bs],
    {reply, ok, State};

handle_call(stop, _From, State = #state{ upstreams = Upstreams }) ->
    [delete_upstream(U) || U <- Upstreams],
    {stop, normal, ok, State};

handle_call(become_real, _From, State) ->
    %% There's a case where we receive this message twice, when the exchange
    %% is created during app startup (which the configured exchnages do...)
    {reply, ok, State};

handle_call(Msg, _From, State) ->
    {stop, {unexpected_call, Msg}, State}.

handle_cast(connect_all, State = #state{upstreams = UpstreamURIs,
                                        downstream_durable = Durable}) ->
    {ok, DConn} = amqp_connection:start(direct),
    {ok, DCh} = amqp_connection:open_channel(DConn),
    #'confirm.select_ok'{} = amqp_channel:call(DCh, #'confirm.select'{}),
    amqp_channel:register_confirm_handler(DCh, self()),
    %%erlang:monitor(process, DCh),
    State0 = State#state{downstream_connection = DConn,
                         downstream_channel = DCh,
                         upstreams = []},
    {noreply,lists:foldl(fun (UpstreamURI, State1) ->
                             connect_upstream(UpstreamURI, not Durable, State1)
                     end, State0, UpstreamURIs)};

handle_cast(Msg, State) ->
    {stop, {unexpected_cast, Msg}, State}.

handle_info(#'basic.consume_ok'{}, State) ->
    {noreply, State};

handle_info(#'basic.ack'{ delivery_tag = Seq, multiple = Multiple },
            State = #state{ upstreams = Upstreams }) ->
    DTagUChUs = retrieve_delivery_tags(Seq, Multiple, Upstreams),
    [amqp_channel:cast(UCh, #'basic.ack'{delivery_tag = DTag,
                                         multiple = Multiple}) ||
        {DTag, UCh, _} <- DTagUChUs, DTag =/= none],
    {noreply, State#state{ upstreams = [U || {_, _, U} <- DTagUChUs] }};

handle_info({#'basic.deliver'{consumer_tag = CTag,
                              delivery_tag = DTag,
                              %% TODO do we care?
                              %%redelivered = Redelivered,
                              %%exchange = Exchange,
                              routing_key = Key}, Msg},
            State = #state{downstream_exchange = #resource{name = X},
                           downstream_channel  = DCh,
                           upstreams           = Upstreams0,
                           next_publish_id     = Seq}) ->
    Headers0 = extract_headers(Msg),
    case forwarded_before(Headers0) of
        false -> {#upstream{uri = URI}, Upstreams} =
                     record_delivery_tag(DTag, CTag, Seq, Upstreams0),
                 %% TODO add user information here?
                 Headers = add_routing_to_headers(Headers0,
                                                  [{<<"uri">>, longstr, URI}]),
                 amqp_channel:cast(DCh, #'basic.publish'{exchange = X,
                                                         routing_key = Key},
                                   update_headers(Headers, Msg)),
                 {noreply, State#state{upstreams       = Upstreams,
                                       next_publish_id = Seq + 1}};
        true  -> {noreply, State}
    end;

handle_info({'DOWN', _Ref, process, Ch, _Reason},
            State = #state{ downstream_channel = DCh }) ->
    case Ch of
        DCh ->
            exit(todo_handle_downstream_channel_death);
        _ ->
            {noreply, restart_upstream_channel(Ch, State)}
    end;

handle_info({connect_upstream, URI, ResetUpstreamQueue}, State) ->
    {noreply, connect_upstream(URI, ResetUpstreamQueue, State)};

handle_info(Msg, State) ->
    {stop, {unexpected_info, Msg}, State}.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

terminate(_Reason, #state { downstream_channel = DCh,
                            downstream_connection = DConn,
                            downstream_exchange = DownstreamX,
                            upstreams = Upstreams }) ->
    ensure_closed(DConn, DCh),
    [ensure_closed(C, Ch) ||
        #upstream{ connection = C, channel = Ch } <- Upstreams],
    true = ets:delete(?ETS_NAME, DownstreamX),
    ok.

%%----------------------------------------------------------------------------

connect_upstream(UpstreamURI, ResetUpstreamQueue,
                 State = #state{ upstreams = Upstreams,
                                 downstream_exchange = DownstreamX}) ->
    %%io:format("Connecting to ~s...~n", [UpstreamURI]),
    X = proplists:get_value(exchange,
                            rabbit_federation_util:parse_uri(UpstreamURI)),
    Params = params_from_uri(UpstreamURI),
    case amqp_connection:start(network, Params) of
        {ok, Conn} ->
            %%io:format("Done!~n", []),
            {ok, Ch} = amqp_connection:open_channel(Conn),
            erlang:monitor(process, Ch),
            Q = upstream_queue_name(X, DownstreamX),
            %% TODO: The x-expires should be configurable.
            case ResetUpstreamQueue of
                true ->
                    with_disposable_channel(
                      Conn,
                      fun (Ch2) ->
                              amqp_channel:call(Ch2, #'queue.delete'{queue = Q})
                      end);
                _ ->
                    ok
            end,
            amqp_channel:call(
              Ch, #'queue.declare'{
                queue     = Q,
                durable   = true,
                arguments = [{<<"x-expires">>, long, 86400000}] }),
            #'basic.consume_ok'{ consumer_tag = CTag } =
                amqp_channel:subscribe(Ch, #'basic.consume'{ queue = Q,
                                                             no_ack = false },
                                       self()),
            State#state{ upstreams =
                             [#upstream{ uri          = UpstreamURI,
                                         connection   = Conn,
                                         channel      = Ch,
                                         queue        = Q,
                                         exchange     = X,
                                         consumer_tag = CTag,
                                         unacked      = gb_trees:empty()}
                              | Upstreams]};
        _E ->
            erlang:send_after(
              1000, self(),
              {connect_upstream, UpstreamURI, ResetUpstreamQueue}),
            State
    end.

upstream_queue_name(X, #resource{ name         = DownstreamName,
                                  virtual_host = DownstreamVHost }) ->
    Node = list_to_binary(atom_to_list(node())),
    <<"federation: ", X/binary, " -> ", Node/binary,
      "-", DownstreamVHost/binary, "-", DownstreamName/binary>>.


is_federation_queue(#resource{ name = <<"federation: ", _Rest/binary>>,
                               kind = queue }) ->
    true;
is_federation_queue(_) ->
    false.

restart_upstream_channel(OldPid, State = #state{ upstreams = Upstreams }) ->
    {[#upstream{ uri = URI }], Rest} =
        lists:partition(fun (#upstream{ channel = Ch }) ->
                                OldPid == Ch
                        end, Upstreams),
    connect_upstream(URI, false, State#state{ upstreams = Rest }).

bind_upstream(#upstream{ channel = Ch, queue = Q, exchange = X },
              Key, Args) ->
    amqp_channel:call(Ch, #'queue.bind'{queue       = Q,
                                        exchange    = X,
                                        routing_key = Key,
                                        arguments   = Args}).

maybe_unbind_upstreams(Upstreams, #binding{source = Source, destination = Dest,
                                           key = Key, args = Args}) ->
    case is_federation_queue(Dest) of
        true  -> ok;
        false -> case lists:any(fun (#binding{ key = Key2, args = Args2 } ) ->
                                        Key == Key2 andalso Args == Args2
                                end,
                                rabbit_binding:list_for_source(Source)) of
                     true  -> ok;
                     false -> [unbind_upstream(U, Key, Args) || U <- Upstreams]
                 end
    end.

unbind_upstream(#upstream{ connection = Conn, queue = Q, exchange = X },
                Key, Args) ->
    %% We may already be unbound if e.g. someone has deleted the upstream
    %% exchange
    with_disposable_channel(
      Conn,
      fun (Ch) ->
              amqp_channel:call(Ch, #'queue.unbind'{queue       = Q,
                                                    exchange    = X,
                                                    routing_key = Key,
                                                    arguments   = Args})
      end).

delete_upstream(#upstream{ connection = Conn, queue = Q }) ->
    with_disposable_channel(
      Conn, fun (Ch) -> amqp_channel:call(Ch, #'queue.delete'{queue = Q}) end).

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

record_delivery_tag(DTag, CTag, Seq, Upstreams) ->
    Us = [record_delivery_tag0(DTag, CTag, Seq, U) || U <- Upstreams],
    [Changed] = [U || {changed, U} <- Us],
    {Changed, [U || {_, U} <- Us]}.

record_delivery_tag0(DTag, CTag, Seq,
                     Upstream = #upstream { consumer_tag = CTag,
                                            unacked      = Unacked }) ->
    {changed, Upstream#upstream{unacked = gb_trees:insert(Seq, DTag, Unacked)}};
record_delivery_tag0(_DTag, _CTag, _Seq, Upstream) ->
    {unchanged, Upstream}.

retrieve_delivery_tags(Seq, Multiple, Upstreams) ->
    [retrieve_delivery_tag(Seq, Multiple, U) || U <- Upstreams].

retrieve_delivery_tag(Seq, Multiple,
                      Upstream = #upstream { channel = UCh,
                                             unacked = Unacked0 }) ->
    Unacked = remove_delivery_tags(Seq, Multiple, Unacked0),
    DTag = case gb_trees:lookup(Seq, Unacked0) of
               {value, V} -> V;
               none       -> none
           end,
    {DTag, UCh, Upstream#upstream{ unacked = Unacked }}.

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
    try amqp_connection:close(Conn)
    catch exit:{noproc, _} -> ok
    end.

ensure_closed(Ch) ->
    try amqp_channel:close(Ch)
    catch exit:{noproc, _} -> ok
    end.

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
