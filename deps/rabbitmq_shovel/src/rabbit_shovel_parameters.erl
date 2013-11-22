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
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2007-2013 GoPivotal, Inc.  All rights reserved.
%%

-module(rabbit_shovel_parameters).
-behaviour(rabbit_runtime_parameter).

-include_lib("amqp_client/include/amqp_client.hrl").
-include("rabbit_shovel.hrl").

-export([validate/4, notify/4, notify_clear/3]).
-export([register/0, parse/1]).

-import(rabbit_misc, [pget/2, pget/3]).

-rabbit_boot_step({?MODULE,
                   [{description, "shovel parameters"},
                    {mfa, {rabbit_shovel_parameters, register, []}},
                    {requires, rabbit_registry},
                    {enables, recovery}]}).

register() ->
    rabbit_registry:register(runtime_parameter, <<"shovel">>, ?MODULE).

validate(_VHost, <<"shovel">>, Name, Definition) ->
    rabbit_parameter_validation:proplist(Name, validation(), Definition);

validate(_VHost, _Component, Name, _Term) ->
    {error, "name not recognised: ~p", [Name]}.

notify(_VHost, <<"shovel">>, Name, Definition) ->
    rabbit_shovel_dyn_worker_sup_sup:adjust_or_start_child(Name, Definition).

notify_clear(_VHost, <<"shovel">>, Name) ->
    rabbit_shovel_dyn_worker_sup_sup:stop_child(Name).

%%----------------------------------------------------------------------------

validation() ->
    [{<<"from-uri">>,       fun validate_uri/2, mandatory},
     {<<"to-uri">>,         fun validate_uri/2, mandatory},
     {<<"from-exchange">>,  fun rabbit_parameter_validation:binary/2, optional},
     {<<"from-exchange-key">>,fun rabbit_parameter_validation:binary/2, optional},
     {<<"from-queue">>,     fun rabbit_parameter_validation:binary/2, optional},
     {<<"to-exchange">>,    fun rabbit_parameter_validation:binary/2, optional},
     {<<"to-exchange-key">>,fun rabbit_parameter_validation:binary/2, optional},
     {<<"to-queue">>,       fun rabbit_parameter_validation:binary/2, optional},
     {<<"prefetch-count">>, fun rabbit_parameter_validation:number/2, optional},
     {<<"reconnect-delay">>,fun rabbit_parameter_validation:number/2, optional},
     {<<"ack-mode">>,       rabbit_parameter_validation:enum(
                              ['no-ack', 'on-publish', 'on-confirm']), optional}
    ].

%% TODO this function is duplicated from federation. Move to amqp_uri module?
validate_uri(Name, Term) when is_binary(Term) ->
    case rabbit_parameter_validation:binary(Name, Term) of
        ok -> case amqp_uri:parse(binary_to_list(Term)) of
                  {ok, _}    -> ok;
                  {error, E} -> {error, "\"~s\" not a valid URI: ~p", [Term, E]}
              end;
        E  -> E
    end;
validate_uri(Name, Term) ->
    case rabbit_parameter_validation:list(Name, Term) of
        ok -> case [V || U <- Term,
                         V <- [validate_uri(Name, U)],
                         element(1, V) =:= error] of
                  []      -> ok;
                  [E | _] -> E
              end;
        E  -> E
    end.

%%----------------------------------------------------------------------------

parse(Def) ->
    {ok, FromParams} = parse_uri(<<"from-uri">>, Def),
    {ok, ToParams}   = parse_uri(<<"to-uri">>,   Def),
    FromExch     = pget(<<"from-exchange">>,     Def, none),
    FromExchKey  = pget(<<"from-exchange-key">>, Def, none),
    FromQueue    = pget(<<"from-queue">>,        Def, <<>>),
    ToExch       = pget(<<"to-exchange">>,       Def, none),
    ToExchKey    = pget(<<"to-exchange-key">>,   Def, none),
    ToQueue      = pget(<<"to-queue">>,          Def, none),
    FromFun = fun (Conn, Ch) ->
                      case FromQueue of
                          <<>> -> Ms = [#'queue.declare'{exclusive = true},
                                        #'queue.bind'{routing_key = FromExchKey,
                                                      exchange    = FromExch}],
                                  [amqp_channel:call(Ch, M) || M <- Ms];
                          _    -> ensure_queue(Conn, FromQueue)
                      end
              end,
    ToFun = fun (Conn, _Ch) ->
                    case ToQueue of
                        none -> ok;
                        _    -> ensure_queue(Conn, ToQueue)
                    end
            end,
    {Exch, Key}  = case ToQueue of
                       none -> {ToExch, ToExchKey};
                       _    -> {<<"">>, ToQueue}
                   end,
    PubFun = fun (P0) -> P1 = case Exch of
                                  none -> P0;
                                  _    -> P0#'basic.publish'{exchange = Exch}
                              end,
                         case Key of
                             none -> P1;
                             _    -> P1#'basic.publish'{routing_key = Key}
                         end
             end,
    {ok, #shovel{
       sources            = #endpoint{amqp_params = [FromParams],
                                      resource_declaration = FromFun},
       destinations       = #endpoint{amqp_params = [ToParams],
                                      resource_declaration = ToFun},
       prefetch_count     = pget(<<"prefetch-count">>, Def, 1000),
       ack_mode           = translate_ack_mode(
                              pget(<<"ack-mode">>, Def, <<"on-confirm">>)),
       publish_fields     = PubFun,
       publish_properties = fun (P) -> P end,
       queue              = FromQueue,
       reconnect_delay    = pget(<<"reconnect-delay">>, Def, 1)}}.

parse_uri(Key, Def) -> amqp_uri:parse(binary_to_list(pget(Key, Def))).

translate_ack_mode(<<"on-confirm">>) -> on_confirm;
translate_ack_mode(<<"on-publish">>) -> on_publish;
translate_ack_mode(<<"no-ack">>)     -> no_ack.

ensure_queue(Conn, Queue) ->
    {ok, Ch} = amqp_connection:open_channel(Conn),
    try
        amqp_channel:call(Ch, #'queue.declare'{queue   = Queue,
                                               passive = true})
    catch exit:{{shutdown, {server_initiated_close, ?NOT_FOUND, _Text}}, _} ->
            {ok, Ch2} = amqp_connection:open_channel(Conn),
            amqp_channel:call(Ch2, #'queue.declare'{queue   = Queue,
                                                   durable = true}),
            catch amqp_channel:close(Ch2)

    after
        catch amqp_channel:close(Ch)
    end.
