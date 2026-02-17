%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term "Broadcom" refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

%% Installs an OTP Logger handler that writes HTTP access logs to a file.
%% Idempotent: safe to call from both rabbit_web_dispatch_app and rabbit_mgmt_sup.

-module(rabbit_web_dispatch_access_log).

-include_lib("kernel/include/logger.hrl").
-include_lib("rabbit_common/include/logging.hrl").

-export([setup/0, cleanup/0, resolve_log_dir/0]).

-ifdef(TEST).
-export([extract_rotation_spec/1]).
-endif.

-define(HANDLER_ID, rabbitmq_http_api_access_log).
-define(ACCESS_LOG_FILE, "access.log").

-spec setup() -> ok.
setup() ->
    LogDir = resolve_log_dir(),
    suppress_http_api_in_global_handlers(),
    case LogDir of
        none -> ok;
        _    -> install_handler(LogDir)
    end.

-spec cleanup() -> ok.
cleanup() ->
    _ = logger:remove_handler(?HANDLER_ID),
    unsuppress_http_api_in_global_handlers(),
    ok.

%% Checks http_dispatch.access_log_dir first, falls back to management.http_log_dir.
-spec resolve_log_dir() -> file:filename_all() | none.
resolve_log_dir() ->
    case application:get_env(rabbitmq_web_dispatch, access_log_dir, none) of
        none ->
            application:get_env(rabbitmq_management, http_log_dir, none);
        Dir ->
            Dir
    end.

install_handler(LogDir) ->
    Filename = filename:join(LogDir, ?ACCESS_LOG_FILE),
    _ = filelib:ensure_dir(Filename),
    RotationConfig = default_rotation_config(),
    HandlerConfig = #{
        level => all,
        filter_default => stop,
        filters => [
            {domain_filter,
             {fun logger_filters:domain/2,
              {log, sub, ?RMQLOG_DOMAIN_HTTP_ACCESS}}}
        ],
        formatter => {rabbit_access_log_fmt, #{}},
        config => RotationConfig#{
            type => file,
            file => Filename
        }
    },
    case logger:add_handler(?HANDLER_ID, rabbit_logger_std_h, HandlerConfig) of
        ok ->
            ok;
        {error, {already_exist, ?HANDLER_ID}} ->
            ok;
        {error, Reason} ->
            ?LOG_ERROR(
               "Failed to set up HTTP API access log handler: ~tp",
               [Reason]),
            ok
    end.

%% Inherits rotation settings from the log.default category.
default_rotation_config() ->
    Env = application:get_env(rabbit, log, []),
    CatProps = proplists:get_value(categories, Env, []),
    DefaultProps = proplists:get_value(default, CatProps, []),
    extract_rotation_spec(DefaultProps).

extract_rotation_spec(Props) ->
    Spec = [{K, V} || {K, V} <- Props,
                      lists:member(K, [rotate_on_date,
                                       compress_on_rotate,
                                       max_no_bytes,
                                       max_no_files])],
    maps:from_list(Spec).

%% Route http_api events only to the access log handler.
suppress_http_api_in_global_handlers() ->
    update_global_handlers(fun(FilterConfig) ->
        FilterConfig#{http_api => none}
    end).

%% Re-enable http_api events in global handlers.
unsuppress_http_api_in_global_handlers() ->
    update_global_handlers(fun(FilterConfig) ->
        maps:remove(http_api, FilterConfig)
    end).

update_global_handlers(UpdateFun) ->
    Handlers = logger:get_handler_config(),
    lists:foreach(
      fun(#{id := Id, filters := Filters}) ->
              case proplists:get_value(?FILTER_NAME, Filters) of
                  undefined ->
                      ok;
                  {Fun, FilterConfig} ->
                      FilterConfig1 = UpdateFun(FilterConfig),
                      case FilterConfig1 =:= FilterConfig of
                          true ->
                              ok;
                          false ->
                              Filters1 = lists:keystore(
                                           ?FILTER_NAME, 1, Filters,
                                           {?FILTER_NAME, {Fun, FilterConfig1}}),
                              logger:set_handler_config(Id, filters, Filters1)
                      end
              end;
         (_) ->
              ok
      end, Handlers).
