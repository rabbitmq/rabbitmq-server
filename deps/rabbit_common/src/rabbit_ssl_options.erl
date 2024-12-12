%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2024 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_ssl_options).

-export([fix/1]).
<<<<<<< HEAD
=======
-export([fix_client/1]).
>>>>>>> 5086e283b (Allow building CLI with elixir 1.18.x)


-define(BAD_SSL_PROTOCOL_VERSIONS, [
                                    %% POODLE
                                    sslv3
                                   ]).

-spec fix(rabbit_types:infos()) -> rabbit_types:infos().

fix(Config) ->
    fix_verify_fun(
      fix_ssl_protocol_versions(
        hibernate_after(Config))).

<<<<<<< HEAD
=======
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

>>>>>>> 5086e283b (Allow building CLI with elixir 1.18.x)
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
            rabbit_log:error("TLS verify_fun: module ~ts missing: ~tp",
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
            rabbit_log:error("TLS verify_fun: no ~ts:~ts/3 exported",
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
