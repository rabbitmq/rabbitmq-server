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

-module(rabbit_federation_exchange).

-rabbit_boot_step({?MODULE,
                   [{description, "federation exchange type"},
                    {mfa, {rabbit_registry, register,
                           [exchange, <<"x-federation">>,
                            rabbit_federation_exchange]}},
                    {requires, rabbit_registry},
                    {enables, exchange_recovery}]}).

-include_lib("rabbit_common/include/rabbit_exchange_type_spec.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

-behaviour(rabbit_exchange_type).
-behaviour(gen_server).

-export([start/0]).

-export([description/0, route/2]).
-export([validate/1, create/2, recover/2, delete/3,
         add_binding/3, remove_bindings/3, assert_args_equivalence/2]).

-export([start_link/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

%%----------------------------------------------------------------------------

-define(ETS_NAME, ?MODULE).
-define(TX, false).
-record(state, { downstream_connection, downstream_channel,
                 downstream_exchange, next_publish_id, upstreams }).
-record(upstream, { connection, channel, queue, properties, uri, unacked,
                    consumer_tag }).

%%----------------------------------------------------------------------------

start() ->
    %% TODO get rid of this ets table when bug 23825 lands.
    ?ETS_NAME = ets:new(?ETS_NAME, [public, set, named_table]).

description() ->
    [{name, <<"x-federation">>},
     {description, <<"Federation exchange">>}].

route(X, Delivery) ->
    with_module(X, fun (M) -> M:route(X, Delivery) end).

validate(X = #exchange{arguments = Args}) ->
    validate_arg(<<"upstreams">>, array,   Args),
    validate_arg(<<"type">>,      longstr, Args),
    {array, Upstreams} = rabbit_misc:table_lookup(Args, <<"upstreams">>),
    [validate_upstream(U) || U <- Upstreams],
    {longstr, TypeBin} = rabbit_misc:table_lookup(Args, <<"type">>),
    Type = rabbit_exchange:check_type(TypeBin),
    case Type of
        'x-federation' -> fail("Type argument must not be x-federation.", []);
        _              -> ok
    end,
    with_module(X, fun (M) -> M:validate(X) end).

create(?TX, X = #exchange{ name = Downstream, durable = Durable,
                           arguments = Args }) ->
    {array, UpstreamURIs0} =
        rabbit_misc:table_lookup(Args, <<"upstreams">>),
    UpstreamURIs = [U || {longstr, U} <- UpstreamURIs0],
    rabbit_federation_sup:start_child({Downstream, Durable, UpstreamURIs}),
    with_module(X, fun (M) -> M:create(?TX, X) end);
create(Tx, X) ->
    with_module(X, fun (M) -> M:create(Tx, X) end).

recover(X, Bs) ->
    with_module(X, fun (M) -> M:recover(X, Bs) end).

delete(?TX, X, Bs) ->
    %% TODO delete upstream queues
    call(X, stop),
    with_module(X, fun (M) -> M:delete(?TX, X, Bs) end);
delete(Tx, X, Bs) ->
    with_module(X, fun (M) -> M:delete(Tx, X, Bs) end).

add_binding(?TX, X, B) ->
    %% TODO add bindings only if needed.
    call(X, {add_binding, B}),
    with_module(X, fun (M) -> M:add_binding(?TX, X, B) end);
add_binding(Tx, X, B) ->
    with_module(X, fun (M) -> M:add_binding(Tx, X, B) end).

remove_bindings(?TX, X, Bs) ->
    %% TODO remove bindings only if needed.
    call(X, {remove_bindings, Bs}),
    with_module(X, fun (M) -> M:remove_bindings(?TX, X, Bs) end);
remove_bindings(Tx, X, Bs) ->
    with_module(X, fun (M) -> M:remove_bindings(Tx, X, Bs) end).

assert_args_equivalence(X = #exchange{name = Name, arguments = Args},
                        NewArgs) ->
    rabbit_misc:assert_args_equivalence(Args, NewArgs, Name,
                                        [<<"upstream">>, <<"type">>]),
    with_module(X, fun (M) -> M:assert_args_equivalence(X, Args) end).

%%----------------------------------------------------------------------------

call(#exchange{ name = Downstream }, Msg) ->
    [{_, Pid}] = ets:lookup(?ETS_NAME, Downstream),
    gen_server:call(Pid, Msg, infinity).

with_module(#exchange{ arguments = Args }, Fun) ->
    %% TODO should this be cached? It's on the publish path.
    {longstr, Type} = rabbit_misc:table_lookup(Args, <<"type">>),
    {ok, Module} = rabbit_registry:lookup_module(
                     exchange, rabbit_exchange:check_type(Type)),
    Fun(Module).

%%----------------------------------------------------------------------------

start_link(Args) ->
    gen_server:start_link(?MODULE, Args, [{timeout, infinity}]).

%%----------------------------------------------------------------------------

init({DownstreamX, Durable, UpstreamURIs}) ->
    {ok, DConn} = amqp_connection:start(direct),
    {ok, DCh} = amqp_connection:open_channel(DConn),
    #'confirm.select_ok'{} = amqp_channel:call(DCh, #'confirm.select'{}),
    amqp_channel:register_confirm_handler(DCh, self()),
    %%erlang:monitor(process, DCh),
    true = ets:insert(?ETS_NAME, {DownstreamX, self()}),
    State0 = #state{downstream_connection = DConn, downstream_channel = DCh,
                    downstream_exchange = DownstreamX, upstreams = [],
                    next_publish_id = 1},
    {ok, lists:foldl(fun (UpstreamURI, State) ->
                             connect_upstream(UpstreamURI, not Durable, State)
                     end, State0, UpstreamURIs)}.

handle_call({add_binding, #binding{key = Key, args = Args} }, _From,
            State = #state{ upstreams = Upstreams }) ->
    [bind_upstream(U, Key, Args) || U <- Upstreams],
    {reply, ok, State};

handle_call({remove_bindings, Bs }, _From,
            State = #state{ upstreams = Upstreams }) ->
    [unbind_upstream(U, Key, Args) ||
        #binding{key = Key, args = Args} <- Bs,
        U                                <- Upstreams],
    {reply, ok, State};

handle_call(stop, _From, State) ->
    {stop, normal, ok, State};

handle_call(Msg, _From, State) ->
    {stop, {unexpected_call, Msg}, State}.

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
                              %%redelivered = Redelivered,
                              %%exchange = Exchange,
                              routing_key = Key},
             Msg}, State = #state{downstream_exchange = #resource {name = X},
                                  downstream_channel  = DCh,
                                  upstreams           = Upstreams0,
                                  next_publish_id     = Seq}) ->
    Upstreams = record_delivery_tag(DTag, CTag, Seq, Upstreams0),
    amqp_channel:cast(DCh, #'basic.publish'{exchange = X,
                                            routing_key = Key}, Msg),
    {noreply, State#state{ upstreams       = Upstreams,
                           next_publish_id = Seq + 1}};

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
    Props = parse_uri(UpstreamURI),
    Params = #amqp_params{host         = proplists:get_value(host, Props),
                          port         = proplists:get_value(port, Props),
                          virtual_host = proplists:get_value(vhost, Props)},
    case amqp_connection:start(network, Params) of
        {ok, Conn} ->
            %%io:format("Done!~n", []),
            {ok, Ch} = amqp_connection:open_channel(Conn),
            erlang:monitor(process, Ch),
            Q = upstream_queue_name(Props, DownstreamX),
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
                                         properties   = Props,
                                         consumer_tag = CTag,
                                         unacked      = gb_trees:empty()}
                              | Upstreams]};
        _E ->
            erlang:send_after(
              1000, self(),
              {connect_upstream, UpstreamURI, ResetUpstreamQueue}),
            State
    end.

upstream_queue_name(Props, #resource{ name         = DownstreamName,
                                      virtual_host = DownstreamVHost }) ->
    X = proplists:get_value(exchange, Props),
    Node = list_to_binary(atom_to_list(node())),
    <<"federation: ", X/binary, " -> ", Node/binary,
      "-", DownstreamVHost/binary, "-", DownstreamName/binary>>.

restart_upstream_channel(OldPid, State = #state{ upstreams = Upstreams }) ->
    {[#upstream{ uri = URI }], Rest} =
        lists:partition(fun (#upstream{ channel = Ch }) ->
                                OldPid == Ch
                        end, Upstreams),
    connect_upstream(URI, false, State#state{ upstreams = Rest }).

bind_upstream(#upstream{ channel = Ch, queue = Q, properties = Props },
              Key, Args) ->
    X = proplists:get_value(exchange, Props),
    amqp_channel:call(Ch, #'queue.bind'{queue       = Q,
                                        exchange    = X,
                                        routing_key = Key,
                                        arguments   = Args}).

unbind_upstream(#upstream{ connection = Conn, queue = Q, properties = Props },
                Key, Args) ->
    X = proplists:get_value(exchange, Props),
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

%%----------------------------------------------------------------------------

record_delivery_tag(DTag, CTag, Seq, Upstreams) ->
    [record_delivery_tag0(DTag, CTag, Seq, U) || U <- Upstreams].

record_delivery_tag0(DTag, CTag, Seq,
                     Upstream = #upstream { consumer_tag = CTag,
                                            unacked      = Unacked }) ->
    Upstream#upstream { unacked = gb_trees:insert(Seq, DTag, Unacked) };
record_delivery_tag0(_DTag, _CTag, _Seq, Upstream) ->
    Upstream.

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

validate_arg(Name, Type, Args) ->
    case rabbit_misc:table_lookup(Args, Name) of
        {Type, _} -> ok;
        undefined -> fail("Argument ~s missing", [Name]);
        _         -> fail("Argument ~s must be of type ~s", [Name, Type])
    end.

validate_upstream({longstr, URI}) ->
    case parse_uri(URI) of
        {error, E} ->
            fail("URI ~s could not be parsed, error: ~p", [URI, E]);
        Props ->
            case proplists:get_value(scheme, Props) of
                "amqp" -> ok;
                S      -> fail("Scheme ~s not supported", [S])
            end
    end;
validate_upstream({Type, URI}) ->
    fail("URI ~w was of type ~s, not longstr", [URI, Type]).

fail(Fmt, Args) ->
    rabbit_misc:protocol_error(precondition_failed, Fmt, Args).

%%----------------------------------------------------------------------------

parse_uri(URI) ->
    case uri_parser:parse(
           binary_to_list(URI), [{host, undefined}, {path, "/"},
                                 {port, 5672},      {'query', []}]) of
        {error, _} = E ->
            E;
        Props ->
            case string:tokens(proplists:get_value(path, Props), "/") of
                [VHostEnc, XEnc] ->
                    VHost = httpd_util:decode_hex(VHostEnc),
                    X = httpd_util:decode_hex(XEnc),
                    [{vhost,    list_to_binary(VHost)},
                     {exchange, list_to_binary(X)}] ++ Props;
                _ ->
                    {error, path_must_have_two_components}
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
