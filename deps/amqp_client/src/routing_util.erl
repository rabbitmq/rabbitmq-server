%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License at
%% http://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
%% License for the specific language governing rights and limitations
%% under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is VMware, Inc.
%% Copyright (c) 2013-2013 VMware, Inc.  All rights reserved.
%%

-module(routing_util).

-export([init_state/0, dest_prefixes/0, all_dest_prefixes/0]).
-export([ensure_endpoint/4, ensure_binding/3]).
-export([parse_endpoint/1, parse_endpoint/2, parse_routing/1]).

-include("amqp_client.hrl").
-include("routing_prefixes.hrl").

%%----------------------------------------------------------------------------

init_state() -> sets:new().

dest_prefixes() -> [?EXCHANGE_PREFIX, ?TOPIC_PREFIX, ?QUEUE_PREFIX,
                    ?AMQQUEUE_PREFIX, ?REPLY_QUEUE_PREFIX].

all_dest_prefixes() -> [?TEMP_QUEUE_PREFIX | dest_prefixes()].


ensure_endpoint(source, Channel, {exchange, _}, State) ->
    #'queue.declare_ok'{queue = Queue} =
        amqp_channel:call(Channel, #'queue.declare'{auto_delete = true,
                                                    exclusive   = true}),
    {ok, Queue, State};

ensure_endpoint(source, Channel, {topic, Name}, State) ->
    ensure_endpoint(source, Channel, {topic, Name, true}, State);
ensure_endpoint(source, Channel, {topic, Name, Durable}, State) ->
    Method =
        case Durable of
            true ->
                Q = list_to_binary(Name),
                #'queue.declare'{durable = true, queue = Q};
            false ->
                #'queue.declare'{auto_delete = true, exclusive = true}
        end,
    #'queue.declare_ok'{queue = Queue} = amqp_channel:call(Channel, Method),
    {ok, Queue, State};

ensure_endpoint(_, Channel, {queue, Name}, State) ->
     Queue = list_to_binary(Name),
     State1 = case sets:is_element(Queue, State) of
                  true -> State;
                  _    -> amqp_channel:cast(Channel,
                                            #'queue.declare'{durable = true,
                                                             queue   = Queue,
                                                             nowait  = true}),
                          sets:add_element(Queue, State)
              end,
    {ok, Queue, State1};

ensure_endpoint(dest, _Channel, {exchange, _}, State) ->
    {ok, undefined, State};

ensure_endpoint(dest, _Ch, {topic, _}, State) ->
    {ok, undefined, State};

ensure_endpoint(_, _Ch, {Type, Name}, State)
  when Type =:= reply_queue orelse Type =:= amqqueue ->
    {ok, list_to_binary(Name), State}.

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

parse_endpoint(Destination, Enc) when is_binary(Destination) ->
    parse_endpoint(unicode:characters_to_list(Destination, Enc)).

parse_endpoint(Destination) when is_list(Destination) ->
    case re:split(Destination, "/", [{return, list}]) of
        [Name] ->
            {ok, {queue, unescape(Name)}};
        ["", Type | Rest]
            when Type =:= "exchange";   Type =:= "queue"; Type =:= "topic";
                 Type =:= "temp-queue"; Type =:= "reply-queue" ->
            parse_endpoint0(atomise(Type), Rest);
        ["", "amq", "queue" | Rest] ->
            parse_endpoint0(amqqueue, Rest);
        _ ->
            {error, {unknown_destination, Destination}}
    end.

parse_endpoint0(exchange, ["" | _] = Rest) ->
    {error, {invalid_destination, exchange, to_url(Rest)}};
parse_endpoint0(exchange, [Name]) ->
    {ok, {exchange, {unescape(Name), undefined}}};
parse_endpoint0(exchange, [Name, Pattern]) ->
    {ok, {exchange, {unescape(Name), unescape(Pattern)}}};
parse_endpoint0(Type, [[_|_]] = [Name]) ->
    {ok, {Type, unescape(Name)}};
parse_endpoint0(Type, Rest) ->
    {error, {invalid_destination, Type, to_url(Rest)}}.

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

%%----------------------------------------------------------------------------

to_url([])  -> [];
to_url(Lol) -> "/" ++ string:join(Lol, "/").

atomise(Name) when is_list(Name) ->
    list_to_atom(re:replace(Name, "-", "_", [{return,list}, global])).

unescape_all(Lol) -> [unescape(L) || L <- Lol].
unescape(Str) -> unescape(Str, []).

unescape("%2F" ++ Str, Acc) -> unescape(Str, [$/ | Acc]);
unescape([C | Str],    Acc) -> unescape(Str, [C | Acc]);
unescape([],           Acc) -> lists:reverse(Acc).

