%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

%% @doc RabbitMQ backend for lager.
%% Configuration is a proplist with the following keys:
%% <ul>
%%    <li>`level' - log level to use</li>
%%    <li>`formatter' - the module to use when formatting log messages. Defaults to
%%                      `lager_default_formatter'</li>
%%    <li>`formatter_config' - the format configuration string. Defaults to
%%                             `time [ severity ] message'</li>
%% </ul>

-module(lager_exchange_backend).

-behaviour(gen_event).

-export([init/1, terminate/2, code_change/3,
         handle_call/2, handle_event/2, handle_info/2]).

-export([maybe_init_exchange/0]).

-include_lib("rabbit_common/include/rabbit.hrl").
-include_lib("rabbit_common/include/rabbit_framing.hrl").

-include_lib("lager/include/lager.hrl").

-record(state, {level :: {'mask', integer()},
                formatter :: atom(),
                format_config :: any(),
                init_exchange_ts = undefined :: integer() | undefined,
                exchange = undefined :: #resource{} | undefined}).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-compile([{parse_transform, lager_transform}]).
-endif.

-define(INIT_EXCHANGE_INTERVAL_SECS, 5).
-define(TERSE_FORMAT, [time, " [", severity, "] ", message]).
-define(DEFAULT_FORMAT_CONFIG, ?TERSE_FORMAT).
-define(FORMAT_CONFIG_OFF, []).

-ifdef(TEST).
-define(DEPRECATED(_Msg), ok).
-else.
-define(DEPRECATED(Msg),
        io:format(user, "WARNING: This is a deprecated lager_exchange_backend configuration. Please use \"~w\" instead.~n", [Msg])).
-endif.

-define(LOG_EXCH_NAME, <<"amq.rabbitmq.log">>).

init([Level]) when is_atom(Level) ->
    ?DEPRECATED([{level, Level}]),
    init([{level, Level}]);
init([Level, true]) when is_atom(Level) -> % for backwards compatibility
    ?DEPRECATED([{level, Level}, {formatter_config, [{eol, "\\r\\n\\"}]}]),
    init([{level, Level}, {formatter_config, ?FORMAT_CONFIG_OFF}]);
init([Level, false]) when is_atom(Level) -> % for backwards compatibility
    ?DEPRECATED([{level, Level}]),
    init([{level, Level}]);

init(Options) when is_list(Options) ->
    true = validate_options(Options),
    Level = get_option(level, Options, undefined),
    try lager_util:config_to_mask(Level) of
        L ->
            DefaultOptions = [{formatter, lager_default_formatter},
                              {formatter_config, ?DEFAULT_FORMAT_CONFIG}],
            [Formatter, Config] = [get_option(K, Options, Default) || {K, Default} <- DefaultOptions],
            State0 = #state{level=L,
                            formatter=Formatter,
                            format_config=Config},
            % NB: this will probably always fail since the / vhost isn't available
            State1 = maybe_init_exchange(State0),
            {ok, State1}
    catch
        _:_ ->
            {error, {fatal, bad_log_level}}
    end;
init(Level) when is_atom(Level) ->
    ?DEPRECATED([{level, Level}]),
    init([{level, Level}]);
init(Other) ->
    {error, {fatal, {bad_lager_exchange_backend_config, Other}}}.

% rabbitmq/rabbitmq-server#1973
% This is called immediatly after the / vhost is created
% or recovered
maybe_init_exchange() ->
    case lists:member(?MODULE, gen_event:which_handlers(lager_event)) of
        true ->
            _ = init_exchange(true),
            ok;
        _ ->
            ok
    end.

validate_options([]) -> true;
validate_options([{level, L}|T]) when is_atom(L) ->
    case lists:member(L, ?LEVELS) of
        false ->
            throw({error, {fatal, {bad_level, L}}});
        true ->
            validate_options(T)
    end;
validate_options([{formatter, M}|T]) when is_atom(M) ->
    validate_options(T);
validate_options([{formatter_config, C}|T]) when is_list(C) ->
    validate_options(T);
validate_options([H|_]) ->
    throw({error, {fatal, {bad_lager_exchange_backend_config, H}}}).

get_option(K, Options, Default) ->
   case lists:keyfind(K, 1, Options) of
       {K, V} -> V;
       false -> Default
   end.

handle_call(get_loglevel, #state{level=Level} = State) ->
    {ok, Level, State};
handle_call({set_loglevel, Level}, State) ->
    try lager_util:config_to_mask(Level) of
        Levels ->
            {ok, ok, State#state{level=Levels}}
    catch
        _:_ ->
            {ok, {error, bad_log_level}, State}
    end;
handle_call(_Request, State) ->
    {ok, ok, State}.

handle_event({log, _Message} = Event, State0) ->
    State1 = maybe_init_exchange(State0),
    handle_log_event(Event, State1);
handle_event(_Event, State) ->
    {ok, State}.

handle_info(_Info, State) ->
    {ok, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% @private
handle_log_event({log, _Message}, #state{exchange=undefined} = State) ->
    % NB: tried to define the exchange but still undefined,
    % so not logging this message. Note: we can't log this dropped
    % message because it will start an infinite loop
    {ok, State};
handle_log_event({log, Message},
    #state{level=L, exchange=LogExch,
           formatter=Formatter, format_config=FormatConfig} = State) ->
    case lager_util:is_loggable(Message, L, ?MODULE) of
        true ->
            %% 0-9-1 says the timestamp is a "64 bit POSIX timestamp". That's
            %% second resolution, not millisecond.
            RoutingKey = rabbit_data_coercion:to_binary(lager_msg:severity(Message)),
            Timestamp = os:system_time(seconds),
            Node = rabbit_data_coercion:to_binary(node()),
            Headers = [{<<"node">>, longstr, Node}],
            AmqpMsg = #'P_basic'{content_type = <<"text/plain">>,
                                 timestamp    = Timestamp,
                                 headers      = Headers},
            Body = rabbit_data_coercion:to_binary(Formatter:format(Message, FormatConfig)),
            case rabbit_basic:publish(LogExch, RoutingKey, AmqpMsg, Body) of
                ok                 -> ok;
                {error, not_found} -> ok
            end,
            {ok, State};
        false ->
            {ok, State}
    end.

%% @private
maybe_init_exchange(#state{exchange=undefined, init_exchange_ts=undefined} = State) ->
    Now = erlang:monotonic_time(second),
    handle_init_exchange(init_exchange(true), Now, State);
maybe_init_exchange(#state{exchange=undefined, init_exchange_ts=Timestamp} = State) ->
    Now = erlang:monotonic_time(second),
    % NB: since we may try to declare the exchange on every log message, this ensures
    % that we only try once every 5 seconds
    HasEnoughTimeElapsed = Now - Timestamp > ?INIT_EXCHANGE_INTERVAL_SECS,
    Result = init_exchange(HasEnoughTimeElapsed),
    handle_init_exchange(Result, Now, State);
maybe_init_exchange(State) ->
    State.

%% @private
init_exchange(true) ->
    {ok, DefaultVHost} = application:get_env(rabbit, default_vhost),
    Exchange = rabbit_misc:r(DefaultVHost, exchange, ?LOG_EXCH_NAME),
    try
        %% durable
        #exchange{} = rabbit_exchange:declare(Exchange, topic, true, false, true, [], ?INTERNAL_USER),
        _ = rabbit_log:info("Declared exchange '~s' in vhost '~s'", [?LOG_EXCH_NAME, DefaultVHost]),
        {ok, Exchange}
    catch
        ErrType:Err ->
            _ = rabbit_log:error("Could not declare exchange '~s' in vhost '~s', reason: ~p:~p",
                             [?LOG_EXCH_NAME, DefaultVHost, ErrType, Err]),
            {ok, undefined}
    end;
init_exchange(_) ->
    {ok, undefined}.

%% @private
handle_init_exchange({ok, undefined}, Now, State) ->
    State#state{init_exchange_ts=Now};
handle_init_exchange({ok, Exchange}, Now, State) ->
    State#state{exchange=Exchange, init_exchange_ts=Now}.

-ifdef(TEST).
console_config_validation_test_() ->
    Good = [{level, info}],
    Bad1 = [{level, foo}],
    Bad2 = [{larval, info}],
    AllGood = [{level, info}, {formatter, my_formatter},
               {formatter_config, ["blort", "garbage"]}],
    [
     ?_assertEqual(true, validate_options(Good)),
     ?_assertThrow({error, {fatal, {bad_level, foo}}}, validate_options(Bad1)),
     ?_assertThrow({error, {fatal, {bad_lager_exchange_backend_config, {larval, info}}}}, validate_options(Bad2)),
     ?_assertEqual(true, validate_options(AllGood))
    ].
-endif.
