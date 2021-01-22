%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2013-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_routing_util).

-export([init_state/0, dest_prefixes/0, all_dest_prefixes/0]).
-export([ensure_endpoint/4, ensure_endpoint/5, ensure_binding/3]).
-export([parse_endpoint/1, parse_endpoint/2]).
-export([parse_routing/1, dest_temp_queue/1]).

-include("amqp_client.hrl").
-include("rabbit_routing_prefixes.hrl").

%%----------------------------------------------------------------------------

init_state() -> sets:new().

dest_prefixes() -> [?EXCHANGE_PREFIX, ?TOPIC_PREFIX, ?QUEUE_PREFIX,
                    ?AMQQUEUE_PREFIX, ?REPLY_QUEUE_PREFIX].

all_dest_prefixes() -> [?TEMP_QUEUE_PREFIX | dest_prefixes()].

%% --------------------------------------------------------------------------

parse_endpoint(Destination) ->
    parse_endpoint(Destination, false).

parse_endpoint(undefined, AllowAnonymousQueue) ->
    parse_endpoint("/queue", AllowAnonymousQueue);

parse_endpoint(Destination, AllowAnonymousQueue) when is_binary(Destination) ->
    parse_endpoint(unicode:characters_to_list(Destination),
                                              AllowAnonymousQueue);
parse_endpoint(Destination, AllowAnonymousQueue) when is_list(Destination) ->
    case re:split(Destination, "/", [{return, list}]) of
        [Name] ->
            {ok, {queue, unescape(Name)}};
        ["", Type | Rest]
            when Type =:= "exchange" orelse Type =:= "queue" orelse
                 Type =:= "topic"    orelse Type =:= "temp-queue" ->
            parse_endpoint0(atomise(Type), Rest, AllowAnonymousQueue);
        ["", "amq", "queue" | Rest] ->
            parse_endpoint0(amqqueue, Rest, AllowAnonymousQueue);
        ["", "reply-queue" = Prefix | [_|_]] ->
            parse_endpoint0(reply_queue,
                            [lists:nthtail(2 + length(Prefix), Destination)],
                            AllowAnonymousQueue);
        _ ->
            {error, {unknown_destination, Destination}}
    end.

parse_endpoint0(exchange, ["" | _] = Rest,    _) ->
    {error, {invalid_destination, exchange, to_url(Rest)}};
parse_endpoint0(exchange, [Name],             _) ->
    {ok, {exchange, {unescape(Name), undefined}}};
parse_endpoint0(exchange, [Name, Pattern],    _) ->
    {ok, {exchange, {unescape(Name), unescape(Pattern)}}};
parse_endpoint0(queue,    [],                 false) ->
    {error, {invalid_destination, queue, []}};
parse_endpoint0(queue,    [],                 true) ->
    {ok, {queue, undefined}};
parse_endpoint0(Type,     [[_|_]] = [Name],   _) ->
    {ok, {Type, unescape(Name)}};
parse_endpoint0(Type,     Rest,               _) ->
    {error, {invalid_destination, Type, to_url(Rest)}}.

%% --------------------------------------------------------------------------

ensure_endpoint(Dir, Channel, Endpoint, State) ->
    ensure_endpoint(Dir, Channel, Endpoint, [], State).

ensure_endpoint(source, Channel, {exchange, {Name, _}}, Params, State) ->
    check_exchange(Name, Channel,
                   proplists:get_value(check_exchange, Params, false)),
    Method = queue_declare_method(#'queue.declare'{}, exchange, Params),
    #'queue.declare_ok'{queue = Queue} = amqp_channel:call(Channel, Method),
    {ok, Queue, State};

ensure_endpoint(source, Channel, {topic, _}, Params, State) ->
    Method = queue_declare_method(#'queue.declare'{}, topic, Params),
    #'queue.declare_ok'{queue = Queue} = amqp_channel:call(Channel, Method),
    {ok, Queue, State};

ensure_endpoint(_Dir, _Channel, {queue, undefined}, _Params, State) ->
    {ok, undefined, State};

ensure_endpoint(_, Channel, {queue, Name}, Params, State) ->
    Params1 = rabbit_misc:pmerge(durable, true, Params),
    Queue = list_to_binary(Name),
    State1 = case sets:is_element(Queue, State) of
                 true -> State;
                 _    -> Method = queue_declare_method(
                                    #'queue.declare'{queue  = Queue,
                                                     nowait = true},
                                    queue, Params1),
                         case Method#'queue.declare'.nowait of
                             true  -> amqp_channel:cast(Channel, Method);
                             false -> amqp_channel:call(Channel, Method)
                         end,
                         sets:add_element(Queue, State)
             end,
    {ok, Queue, State1};

ensure_endpoint(dest, Channel, {exchange, {Name, _}}, Params, State) ->
    check_exchange(Name, Channel,
                   proplists:get_value(check_exchange, Params, false)),
    {ok, undefined, State};

ensure_endpoint(dest, _Ch, {topic, _}, _Params, State) ->
    {ok, undefined, State};

ensure_endpoint(_, _Ch, {amqqueue, Name}, _Params, State) ->
  {ok, list_to_binary(Name), State};

ensure_endpoint(_, _Ch, {reply_queue, Name}, _Params, State) ->
  {ok, list_to_binary(Name), State};

ensure_endpoint(_Direction, _Ch, _Endpoint, _Params, _State) ->
    {error, invalid_endpoint}.

%% --------------------------------------------------------------------------

ensure_binding(QueueBin, {"", Queue}, _Channel) ->
    %% i.e., we should only be asked to bind to the default exchange a
    %% queue with its own name
    QueueBin = list_to_binary(Queue),
    ok;
ensure_binding(Queue, {Exchange, RoutingKey}, Channel) ->
    #'queue.bind_ok'{} =
        amqp_channel:call(Channel,
                          #'queue.bind'{
                            queue       = Queue,
                            exchange    = list_to_binary(Exchange),
                            routing_key = list_to_binary(RoutingKey)}),
    ok.

%% --------------------------------------------------------------------------

parse_routing({exchange, {Name, undefined}}) ->
    {Name, ""};
parse_routing({exchange, {Name, Pattern}}) ->
    {Name, Pattern};
parse_routing({topic, Name}) ->
    {"amq.topic", Name};
parse_routing({Type, Name})
  when Type =:= queue orelse Type =:= reply_queue orelse Type =:= amqqueue ->
    {"", Name}.

dest_temp_queue({temp_queue, Name}) -> Name;
dest_temp_queue(_)                  -> none.

%% --------------------------------------------------------------------------

check_exchange(_,            _,       false) ->
    ok;
check_exchange(ExchangeName, Channel, true) ->
    XDecl = #'exchange.declare'{ exchange = list_to_binary(ExchangeName),
                                 passive = true },
    #'exchange.declare_ok'{} = amqp_channel:call(Channel, XDecl),
    ok.

update_queue_declare_arguments(Method, Params) ->
    Method#'queue.declare'{arguments =
                               proplists:get_value(arguments, Params, [])}.

update_queue_declare_exclusive(Method, Params) ->
    case proplists:get_value(exclusive, Params) of
        undefined -> Method;
        Val       -> Method#'queue.declare'{exclusive = Val}
    end.

update_queue_declare_auto_delete(Method, Params) ->
    case proplists:get_value(auto_delete, Params) of
        undefined -> Method;
        Val       -> Method#'queue.declare'{auto_delete = Val}
    end.

update_queue_declare_nowait(Method, Params) ->
    case proplists:get_value(nowait, Params) of
        undefined -> Method;
        Val       -> Method#'queue.declare'{nowait = Val}
    end.

queue_declare_method(#'queue.declare'{} = Method, Type, Params) ->
    %% defaults
    Method1 = case proplists:get_value(durable, Params, false) of
                  true  -> Method#'queue.declare'{durable     = true};
                  false -> Method#'queue.declare'{auto_delete = true,
                                                  exclusive   = true}
              end,
    %% set the rest of queue.declare fields from Params
    Method2 = lists:foldl(fun (F, Acc) -> F(Acc, Params) end,
                Method1, [fun update_queue_declare_arguments/2,
                          fun update_queue_declare_exclusive/2,
                          fun update_queue_declare_auto_delete/2,
                          fun update_queue_declare_nowait/2]),
    case  {Type, proplists:get_value(subscription_queue_name_gen, Params)} of
        {topic, SQNG} when is_function(SQNG) ->
            Method2#'queue.declare'{queue = SQNG()};
        {exchange, SQNG} when is_function(SQNG) ->
            Method2#'queue.declare'{queue = SQNG()};
        _ ->
            Method2
    end.

%% --------------------------------------------------------------------------

to_url([])  -> [];
to_url(Lol) -> "/" ++ string:join(Lol, "/").

atomise(Name) when is_list(Name) ->
    list_to_atom(re:replace(Name, "-", "_", [{return,list}, global])).

unescape(Str) -> unescape(Str, []).

unescape("%2F" ++ Str, Acc) -> unescape(Str, [$/ | Acc]);
unescape([C | Str],    Acc) -> unescape(Str, [C | Acc]);
unescape([],           Acc) -> lists:reverse(Acc).
