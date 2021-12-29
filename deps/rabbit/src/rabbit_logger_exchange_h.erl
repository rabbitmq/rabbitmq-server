%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_logger_exchange_h).

-include_lib("kernel/include/logger.hrl").

-include_lib("rabbit_common/include/rabbit.hrl").
-include_lib("rabbit_common/include/rabbit_framing.hrl").
-include_lib("rabbit_common/include/logging.hrl").

%% logger callbacks
-export([log/2, adding_handler/1, removing_handler/1, changing_config/3,
         filter_config/1]).

-define(DECL_EXCHANGE_INTERVAL_SECS, 5).
-define(LOG_EXCH_NAME, <<"amq.rabbitmq.log">>).
-define(DEFAULT_FORMATTER, logger_formatter).
-define(DEFAULT_FORMATTER_CONFIG, #{}).

%% -------------------------------------------------------------------
%% Logger handler callbacks.
%% -------------------------------------------------------------------

adding_handler(Config) ->
    Config1 = start_setup_proc(Config),
    {ok, Config1}.

changing_config(_SetOrUpdate, OldConfig, _NewConfig) ->
    {ok, OldConfig}.

filter_config(Config) ->
    Config.

log(#{meta := #{mfa := {?MODULE, _, _}}}, _) ->
    ok;
log(LogEvent, Config) ->
    case rabbit_boot_state:get() of
        ready -> do_log(LogEvent, Config);
        _     -> ok
    end.

do_log(LogEvent, #{config := #{exchange := Exchange}} = Config) ->
    RoutingKey = make_routing_key(LogEvent, Config),
    AmqpMsg = log_event_to_amqp_msg(LogEvent, Config),
    Body = try_format_body(LogEvent, Config),
    case rabbit_basic:publish(Exchange, RoutingKey, AmqpMsg, Body) of
        ok                 -> ok;
        {error, not_found} -> ok
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
    Formatted = try_format_body(LogEvent, Formatter, FormatterConfig),
    erlang:iolist_to_binary(Formatted).

try_format_body(LogEvent, Formatter, FormatterConfig) ->
    try
        Formatter:format(LogEvent, FormatterConfig)
    catch
        C:R:S ->
            case {?DEFAULT_FORMATTER, ?DEFAULT_FORMATTER_CONFIG} of
                {Formatter, FormatterConfig} ->
                    "DEFAULT FORMATTER CRASHED\n";
                {DefaultFormatter, DefaultFormatterConfig} ->
                    Msg = {"FORMATTER CRASH: ~tp -- ~p:~p:~p",
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

    Pid = spawn(fun() -> setup_proc(Config#{config => InternalConfig1}) end),
    InternalConfig2 = InternalConfig1#{setup_proc => Pid},
    Config#{config => InternalConfig2}.

setup_proc(
  #{config := #{exchange := #resource{name = Name,
                                      virtual_host = VHost}}} = Config) ->
    case declare_exchange(Config) of
        ok ->
            ?LOG_INFO(
               "Logging to exchange '~s' in vhost '~s' ready", [Name, VHost],
               #{domain => ?RMQLOG_DOMAIN_GLOBAL});
        error ->
            ?LOG_DEBUG(
               "Logging to exchange '~s' in vhost '~s' not ready, "
               "trying again in ~b second(s)",
               [Name, VHost, ?DECL_EXCHANGE_INTERVAL_SECS],
               #{domain => ?RMQLOG_DOMAIN_GLOBAL}),
            receive
                stop -> ok
            after ?DECL_EXCHANGE_INTERVAL_SECS * 1000 ->
                      setup_proc(Config)
            end
    end.

declare_exchange(
  #{config := #{exchange := #resource{name = Name,
                                      virtual_host = VHost} = Exchange}}) ->
    try
        %% Durable.
        #exchange{} = rabbit_exchange:declare(
                        Exchange, topic, true, false, true, [],
                        ?INTERNAL_USER),
        ?LOG_DEBUG(
           "Declared exchange '~s' in vhost '~s'",
           [Name, VHost],
           #{domain => ?RMQLOG_DOMAIN_GLOBAL}),
        ok
    catch
        Class:Reason ->
            ?LOG_DEBUG(
               "Could not declare exchange '~s' in vhost '~s', "
               "reason: ~0p:~0p",
               [Name, VHost, Class, Reason],
               #{domain => ?RMQLOG_DOMAIN_GLOBAL}),
           error
    end.

unconfigure_exchange(
  #{config := #{exchange := #resource{name = Name,
                                      virtual_host = VHost} = Exchange,
                setup_proc := Pid}}) ->
    Pid ! stop,
    rabbit_exchange:delete(Exchange, false, ?INTERNAL_USER),
    ?LOG_INFO(
       "Logging to exchange '~s' in vhost '~s' disabled",
       [Name, VHost],
       #{domain => ?RMQLOG_DOMAIN_GLOBAL}).
