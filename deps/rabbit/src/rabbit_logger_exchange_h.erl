%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_logger_exchange_h).

-include_lib("kernel/include/logger.hrl").

-include_lib("rabbit_common/include/rabbit.hrl").
-include_lib("rabbit_common/include/rabbit_framing.hrl").
-include_lib("rabbit_common/include/logging.hrl").
%% logger callbacks
-export([log/2, adding_handler/1, removing_handler/1, changing_config/3,
         filter_config/1]).
%% Boot step callback.
-export([declare_exchange/0]).
-export([exchange/0]).

-define(LOG_EXCH_NAME, <<"amq.rabbitmq.log">>).
-define(DEFAULT_FORMATTER, logger_formatter).
-define(DEFAULT_FORMATTER_CONFIG, #{}).

%% -------------------------------------------------------------------
%% Logger handler callbacks.
%% -------------------------------------------------------------------

adding_handler(Config) ->
    Config1 = start_setup_proc(Config),
    {ok, Config1}.

changing_config(_SetOrUpdate, OldConfig, NewConfig) ->
    %% Keep exchange and setup_proc unchanged in the internal config,
    %% if they are defined.
    #{config := OldInternalConfig} = OldConfig,
    #{config := NewInternalConfig0} = NewConfig,
    NewInternalConfig = maps:merge(NewInternalConfig0, maps:with([exchange, setup_proc], OldInternalConfig)),
    {ok, NewConfig#{config := NewInternalConfig}}.

filter_config(Config) ->
    Config.

log(#{meta := #{mfa := {?MODULE, _, _}}}, _) ->
    ok;
log(LogEvent, Config) ->
    %% Publishing the log message to an exchange might trigger more logging,
    %% triggering an infinite logging loop. To prevent that, we make use the
    %% process dictionary to record the fact that this logger was already
    %% entered. If that's the case when this function is called, we just drop
    %% the log event.
    Key = ?MODULE,
    ReEntered = erlang:get(Key) =/= undefined,
    case rabbit_boot_state:get() of
        ready when not ReEntered ->
            erlang:put(Key, ?FUNCTION_NAME),
            try
                do_log(LogEvent, Config)
            catch
                C:R:S ->
                    %% don't let logging crash, because then OTP logger
                    %% removes the logger_exchange handler, which in
                    %% turn deletes the log exchange and its bindings
                    erlang:display({?MODULE, crashed, {C, R, S}})
            after
                erlang:erase(Key)
            end,
            ok;
        _ -> ok
    end.

do_log(
  LogEvent,
  #{config := #{exchange := Exchange,
                setup_proc := Pid}} = Config) ->
    RoutingKey = make_routing_key(LogEvent, Config),
    PBasic = log_event_to_amqp_msg(LogEvent, Config),
    Body = try_format_body(LogEvent, Config),
    Content = rabbit_basic:build_content(PBasic, Body),
    case mc_amqpl:message(Exchange, RoutingKey, Content) of
        {ok, Msg} ->
            %% Publishing a message might involve an Erlang process, like a Ra
            %% server process, to log something and call itself. We need to
            %% publish the message asynchronously from a separate process and
            %% ignore the fate of that publish, to not block an Erlang
            %% process.
            Pid ! {publish, Msg},
            ok;
        {error, _Reason} ->
            %% it would be good to log this error but can we?
            ok
    end.

removing_handler(Config) ->
    unconfigure_exchange(Config),
    ok.

%% -------------------------------------------------------------------
%% Internal functions.
%% -------------------------------------------------------------------

log_event_to_amqp_msg(LogEvent, Config) ->
    ContentType = guess_content_type(Config),
    Timestamp = make_timestamp(LogEvent, Config),
    Headers = make_headers(LogEvent, Config),
    #'P_basic'{
       content_type = ContentType,
       timestamp = Timestamp,
       headers = Headers
      }.

make_routing_key(#{level := Level}, _) ->
    rabbit_data_coercion:to_binary(Level).

guess_content_type(#{formatter := {rabbit_logger_json_fmt, _}}) ->
    <<"application/json">>;
guess_content_type(_) ->
    <<"text/plain">>.

make_timestamp(#{meta := #{time := Timestamp}}, _) ->
    erlang:convert_time_unit(Timestamp, microsecond, second);
make_timestamp(_, _) ->
     os:system_time(second).

make_headers(_, _) ->
    Node = rabbit_data_coercion:to_binary(node()),
    [{<<"node">>, longstr, Node}].

try_format_body(LogEvent, #{formatter := {Formatter, FormatterConfig}}) ->
    try_format_body(LogEvent, Formatter, FormatterConfig).

try_format_body(LogEvent, Formatter, FormatterConfig) ->
    try
        Formatted = Formatter:format(LogEvent, FormatterConfig),
        case unicode:characters_to_binary(Formatted) of
            Binary when is_binary(Binary) ->
                Binary;
            Error ->
                %% The formatter returned invalid or incomplete unicode
                throw(Error)
        end
    catch
        C:R:S ->
            case {?DEFAULT_FORMATTER, ?DEFAULT_FORMATTER_CONFIG} of
                {Formatter, FormatterConfig} ->
                    "DEFAULT FORMATTER CRASHED\n";
                {DefaultFormatter, DefaultFormatterConfig} ->
                    Msg = {"FORMATTER CRASH: ~tp -- ~tp:~tp:~tp",
                           [maps:get(msg, LogEvent), C, R, S]},
                    LogEvent1 = LogEvent#{msg => Msg},
                    try_format_body(
                      LogEvent1,
                      DefaultFormatter,
                      DefaultFormatterConfig)
            end
    end.

start_setup_proc(#{config := InternalConfig} = Config) ->
    {ok, DefaultVHost} = application:get_env(rabbit, default_vhost),
    Exchange = rabbit_misc:r(DefaultVHost, exchange, ?LOG_EXCH_NAME),
    InternalConfig1 = InternalConfig#{exchange => Exchange},
    Pid = spawn(fun() ->
                        setup_proc(Config#{config => InternalConfig1})
                end),
    InternalConfig2 = InternalConfig1#{setup_proc => Pid},
    Config#{config => InternalConfig2}.

setup_proc(#{id := Id} = Config) ->
    %% We register this process using the logger handler ID. It makes
    %% debugging convenient but it's not critical. That's why we catch any
    %% exceptions and ignore the return value.
    _ = catch erlang:register(Id, self()),
    loop(Config).

loop(#{config := #{exchange := Exchange}} = Config) ->
    receive
        {publish, Msg} ->
            _ = rabbit_queue_type:publish_at_most_once(Exchange, Msg),
            loop(Config);
        stop ->
            ok
    end.

exchange() ->
    {ok, DefaultVHost} = application:get_env(rabbit, default_vhost),
    Exchange = rabbit_misc:r(DefaultVHost, exchange, ?LOG_EXCH_NAME),
    Exchange.

declare_exchange() ->
    Exchange = exchange(),
    try
        Ret = rabbit_exchange:declare(
                Exchange, topic, true, false, true, [], ?INTERNAL_USER),
        case Ret of
            {ok, #exchange{}} ->
                ?LOG_DEBUG(
                   "Declared ~ts",
                   [rabbit_misc:rs(Exchange)],
                   #{domain => ?RMQLOG_DOMAIN_GLOBAL}),
                ok;
            {error, timeout} ->
                ?LOG_DEBUG(
                   "Could not declare ~ts because the operation timed out",
                   [rabbit_misc:rs(Exchange)],
                   #{domain => ?RMQLOG_DOMAIN_GLOBAL}),
                ok
        end
    catch
        Class:Reason ->
            ?LOG_DEBUG(
               "Could not declare ~ts, reason: ~0p:~0p",
               [rabbit_misc:rs(Exchange), Class, Reason],
               #{domain => ?RMQLOG_DOMAIN_GLOBAL}),
           ok
    end.

unconfigure_exchange(
  #{config := #{exchange := Exchange,
                setup_proc := Pid}}) ->
    Pid ! stop,
    ?LOG_INFO(
       "Logging to ~ts disabled",
       [rabbit_misc:rs(Exchange)],
       #{domain => ?RMQLOG_DOMAIN_GLOBAL}).
