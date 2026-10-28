%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_ssl_options).

-include_lib("kernel/include/logger.hrl").
-include_lib("kernel/include/file.hrl").


-export([
    fix/1,
    fix_client/1,
    wrap_password_opt/1,
    check_file/1,
    check_file_options/1
]).

%% `ssl` reads these files whole, and devices such as `/dev/zero` never end.
-define(MAX_FILE_SIZE, 4 * 1024 * 1024).

-define(BAD_SSL_PROTOCOL_VERSIONS, [
                                    %% POODLE
                                    sslv3
                                   ]).

-type tls_opts() :: [ssl:tls_server_option()] | [ssl:tls_client_option()].

-spec check_file(file:filename_all()) ->
    ok | {error, not_a_regular_file | empty | too_large | file:posix() | badarg}.
check_file(Path) ->
    case file:read_file_info(Path) of
        {ok, #file_info{type = regular, size = 0}} ->
            {error, empty};
        {ok, #file_info{type = regular, size = Size}} when Size =< ?MAX_FILE_SIZE ->
            ok;
        {ok, #file_info{type = regular}} ->
            {error, too_large};
        {ok, #file_info{}} ->
            {error, not_a_regular_file};
        {error, _} = Error ->
            Error
    end.

-spec check_file_options(tls_opts()) ->
    ok | {error, {cacertfile | certfile | keyfile, term()}}.
check_file_options([{Key, Path} | Rest]) when Key =:= cacertfile;
                                               Key =:= certfile;
                                               Key =:= keyfile ->
    case check_file(Path) of
        ok              -> check_file_options(Rest);
        {error, Reason} -> {error, {Key, Reason}}
    end;
check_file_options([_ | Rest]) ->
    check_file_options(Rest);
check_file_options([]) ->
    ok.

-spec wrap_password_opt(tls_opts()) -> tls_opts().
wrap_password_opt(Opts0) ->
    case proplists:get_value(password, Opts0) of
        undefined ->
            Opts0;
        Fun when is_function(Fun) ->
            Opts0;
        Password ->
            %% A password can be a value or a function returning that value.
            %% See the key_pem_password/0 type in https://github.com/erlang/otp/pull/5843/files.
            NewOpts = proplists:delete(password, Opts0),
            Fun = fun() -> Password end,
            [{password, Fun} | NewOpts]
    end.

-spec fix(rabbit_types:infos()) -> rabbit_types:infos().
fix(Config) ->
    fix_verify_fun(
      fix_ssl_protocol_versions(
        fix_log_level(
          hibernate_after(Config)))).

-spec fix_client(rabbit_types:infos()) -> rabbit_types:infos().
fix_client(Config) ->
    fix_cacerts(
        fix(Config)).

fix_cacerts(SslOptsConfig) ->
    CACerts = proplists:get_value(cacerts, SslOptsConfig, undefined),
    CACertfile = proplists:get_value(cacertfile, SslOptsConfig, undefined),
    case {CACerts, CACertfile} of
        {undefined, undefined} ->
            try public_key:cacerts_get() of
                CaCerts ->
                    [{cacerts, CaCerts} | SslOptsConfig]
            catch
                _ ->
                    SslOptsConfig
            end;
        _CaCerts ->
            SslOptsConfig
    end.

fix_verify_fun(SslOptsConfig) ->
    %% Starting with ssl 4.0.1 in Erlang R14B, the verify_fun function
    %% takes 3 arguments and returns a tuple.
    case rabbit_misc:pget(verify_fun, SslOptsConfig) of
        {Module, Function, InitialUserState} ->
            Fun = make_verify_fun(Module, Function, InitialUserState),
            rabbit_misc:pset(verify_fun, Fun, SslOptsConfig);
        {Module, Function} when is_atom(Module) ->
            Fun = make_verify_fun(Module, Function, none),
            rabbit_misc:pset(verify_fun, Fun, SslOptsConfig);
        {Verifyfun, _InitialUserState} when is_function(Verifyfun, 3) ->
            SslOptsConfig;
        undefined ->
            SslOptsConfig
    end.

make_verify_fun(Module, Function, InitialUserState) ->
    try
        %% Preload the module: it is required to use
        %% erlang:function_exported/3.
        Module:module_info()
    catch
        _:Exception ->
            ?LOG_ERROR("TLS verify_fun: module ~ts missing: ~tp",
                             [Module, Exception]),
            throw({error, {invalid_verify_fun, missing_module}})
    end,
    NewForm = erlang:function_exported(Module, Function, 3),
    OldForm = erlang:function_exported(Module, Function, 1),
    case {NewForm, OldForm} of
        {true, _} ->
            %% This verify_fun is supported by Erlang R14B+ (ssl
            %% 4.0.1 and later).
            Fun = fun(OtpCert, Event, UserState) ->
                    Module:Function(OtpCert, Event, UserState)
            end,
            {Fun, InitialUserState};
        {_, true} ->
            %% This verify_fun is supported by Erlang R14B+ for
            %% undocumented backward compatibility.
            %%
            %% InitialUserState is ignored in this case.
            fun(Args) ->
                    Module:Function(Args)
            end;
        _ ->
            ?LOG_ERROR("TLS verify_fun: no ~ts:~ts/3 exported",
              [Module, Function]),
            throw({error, {invalid_verify_fun, function_not_exported}})
    end.

fix_ssl_protocol_versions(Config) ->
    case application:get_env(rabbit, ssl_allow_poodle_attack) of
        {ok, true} ->
            Config;
        _ ->
            Configured = case rabbit_misc:pget(versions, Config) of
                             undefined -> rabbit_misc:pget(available,
                                                           ssl:versions(),
                                                           []);
                             Vs        -> Vs
                         end,
            rabbit_misc:pset(versions, Configured -- ?BAD_SSL_PROTOCOL_VERSIONS, Config)
    end.

hibernate_after(Config) ->
    Key = hibernate_after,
    case proplists:is_defined(Key, Config) of
        true ->
            Config;
        false ->
            [{Key, 6_000} | Config]
    end.

%% Suppress OTP's TLS state machine process notices by defaulting to warning
%% level. RabbitMQ logs all relevant TLS handshake errors itself, so the
%% OTP-level notices for the same events are redundant. Users can override
%% this by setting the log_level SSL option explicitly.
fix_log_level(Config) ->
    case proplists:is_defined(log_level, Config) of
        true  -> Config;
        false -> [{log_level, warning} | Config]
    end.
