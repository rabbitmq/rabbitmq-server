%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_auth_backend_ldap_mgmt).

-behaviour(rabbit_mgmt_extension).

-export([dispatcher/0, web_ui/0]).

-export([init/2,
         content_types_accepted/2,
         allowed_methods/2,
         resource_exists/2,
         is_authorized/2,
         accept_content/2]).


-include_lib("kernel/include/logger.hrl").
-include_lib("rabbitmq_web_dispatch/include/rabbitmq_web_dispatch_records.hrl").

dispatcher() -> [{"/ldap/validate/simple-bind", ?MODULE, []}].

web_ui() -> [].

%%--------------------------------------------------------------------

init(Req, _Opts) ->
    {cowboy_rest, rabbit_mgmt_cors:set_headers(Req, ?MODULE), #context{}}.

content_types_accepted(ReqData, Context) ->
   {[{'*', accept_content}], ReqData, Context}.

allowed_methods(ReqData, Context) ->
    {[<<"PUT">>, <<"OPTIONS">>], ReqData, Context}.

resource_exists(ReqData, Context) ->
    {true, ReqData, Context}.

is_authorized(ReqData, Context) ->
    rabbit_mgmt_util:is_authorized(ReqData, Context).

accept_content(ReqData0, Context) ->
    F = fun (_Values, BodyMap, ReqData1) ->
        try
            Port = safe_parse_int(maps:get(port, BodyMap, 389), "port"),
            UseSsl = safe_parse_bool(maps:get(use_ssl, BodyMap, false), "use_ssl"),
            UseStartTls = safe_parse_bool(maps:get(use_starttls, BodyMap, false), "use_starttls"),
            Servers = maps:get(servers, BodyMap, []),
            UserDN = maps:get(user_dn, BodyMap, <<"">>),
            Password = maps:get(password, BodyMap, <<"">>),
            Options0 = [
                {port, Port},
                {timeout, 5000}
            ],
            {ok, Options1} = maybe_add_ssl_options(Options0, UseSsl, BodyMap),
            case eldap:open(Servers, Options1) of
                {ok, LDAP} ->
                    Result = case maybe_starttls(LDAP, UseStartTls, BodyMap) of
                        ok ->
                            case eldap:simple_bind(LDAP, UserDN, Password) of
                                ok ->
                                    {true, ReqData1, Context};
                                {error, invalidCredentials} ->
                                    rabbit_mgmt_util:unprocessable_entity("invalid LDAP credentials: "
                                                                          "authentication failure",
                                                                          ReqData1, Context);
                                {error, unwillingToPerform} ->
                                    rabbit_mgmt_util:unprocessable_entity("invalid LDAP credentials: "
                                                                          "authentication failure",
                                                                          ReqData1, Context);
                                {error, invalidDNSyntax} ->
                                    rabbit_mgmt_util:unprocessable_entity("invalid LDAP credentials: "
                                                                          "DN syntax invalid / too long",
                                                                          ReqData1, Context);
                                {error, E} ->
                                    Reason = unicode_format(E),
                                    rabbit_mgmt_util:unprocessable_entity(Reason, ReqData1, Context)
                            end;
                        {error, tls_already_started} ->
                            rabbit_mgmt_util:unprocessable_entity("TLS configuration error: "
                                                                  "cannot use StartTLS on an SSL connection "
                                                                  "(use_ssl and use_starttls cannot both be true)",
                                                                  ReqData1, Context);
                        Error ->
                            Reason = unicode_format(Error),
                            rabbit_mgmt_util:unprocessable_entity(Reason, ReqData1, Context)
                    end,
                    eldap:close(LDAP),
                    Result;
                {error, E} ->
                    Reason = unicode_format("LDAP connection failed: ~tp "
                                            "(servers: ~tp, user_dn: ~ts, password: ~s)",
                                            [E, Servers, UserDN, format_password_for_logging(Password)]),
                    rabbit_mgmt_util:bad_request(Reason, ReqData1, Context)
            end
        catch throw:{bad_request, ErrMsg} ->
            rabbit_mgmt_util:bad_request(ErrMsg, ReqData1, Context)
        end
    end,
    rabbit_mgmt_util:with_decode([], ReqData0, Context, F).

%%--------------------------------------------------------------------

maybe_starttls(_LDAP, false, _BodyMap) ->
    ok;
maybe_starttls(LDAP, true, BodyMap) ->
    {ok, TlsOptions} = tls_options(BodyMap),
    eldap:start_tls(LDAP, TlsOptions, 5000).

maybe_add_ssl_options(Options0, false, _BodyMap) ->
    {ok, Options0};
maybe_add_ssl_options(Options0, true, BodyMap) ->
    case maps:is_key(ssl_options, BodyMap) of
        false ->
            {ok, Options0};
        true ->
            Options1 = [{ssl, true} | Options0],
            {ok, TlsOptions} = tls_options(BodyMap),
            Options2 = [{sslopts, TlsOptions} | Options1],
            {ok, Options2}
    end.

tls_options(BodyMap) when is_map_key(ssl_options, BodyMap) ->
    SslOptionsMap = maps:get(ssl_options, BodyMap),
    case is_map(SslOptionsMap) of
        false ->
            throw({bad_request, "ssl_options must be a map/object"});
        true ->
            ok
    end,
    CaCertfile = maps:get(<<"cacertfile">>, SslOptionsMap, undefined),
    CaCertPemData = maps:get(<<"cacert_pem_data">>, SslOptionsMap, undefined),
    TlsOpts0 = case {CaCertfile, CaCertPemData} of
        {undefined, undefined} ->
            [{cacerts, public_key:cacerts_get()}];
        _ ->
            []
    end,
    %% NB: for some reason the "cacertfile" key isn't turned into an atom
    TlsOpts1 = case CaCertfile of
        undefined ->
            TlsOpts0;
        CaCertfile ->
            [{cacertfile, CaCertfile} | TlsOpts0]
    end,
    TlsOpts2 = case CaCertPemData of
        undefined ->
            TlsOpts1;
        CaCertPems when is_list(CaCertPems) ->
            F0 = fun (P) ->
                try
                    case public_key:pem_decode(P) of
                        [{'Certificate', CaCertDerEncoded, not_encrypted}] ->
                            {true, CaCertDerEncoded};
                        [] ->
                            throw({bad_request, "invalid PEM data in cacert_pem_data: "
                                                "no valid certificates found"});
                        _Unexpected ->
                            throw({bad_request, "unexpected cacert_pem_data passed to "
                                                "/ldap/validate/simple-bind ssl_options.cacerts"})
                    end
                catch
                    error:Reason ->
                        throw({bad_request, unicode_format("invalid PEM data in cacert_pem_data: ~tp", [Reason])})
                end
            end,
            CaCertsDerEncoded = lists:filtermap(F0, CaCertPems),
            [{cacerts, CaCertsDerEncoded} | TlsOpts1];
        _ ->
            TlsOpts1
    end,
    TlsOpts3 = case maps:get(<<"verify">>, SslOptionsMap, undefined) of
        undefined ->
            TlsOpts2;
        Verify ->
            try
                VerifyStr = unicode:characters_to_list(Verify),
                [{verify, list_to_existing_atom(VerifyStr)} | TlsOpts2]
            catch
                error:badarg ->
                    throw({bad_request, "invalid verify option passed to "
                                        "/ldap/validate/simple-bind ssl_options.verify"})
            end
    end,
    TlsOpts4 = case maps:get(<<"server_name_indication">>, SslOptionsMap, disable) of
        disable ->
            TlsOpts3;
        SniValue ->
            try
                SniStr = unicode:characters_to_list(SniValue),
                [{server_name_indication, SniStr} | TlsOpts3]
            catch
                error:badarg ->
                    throw({bad_request, "invalid server_name_indication: expected string"});
                error:_ ->
                    throw({bad_request, "invalid server_name_indication: expected string"})
            end
    end,
    TlsOpts5 = case maps:get(<<"depth">>, SslOptionsMap, undefined) of
        undefined ->
            TlsOpts4;
        DepthValue ->
            Depth = safe_parse_int(DepthValue, "ssl_options.depth"),
            [{depth, Depth} | TlsOpts4]
    end,
    TlsOpts6 = case maps:get(<<"versions">>, SslOptionsMap, undefined) of
        undefined ->
            TlsOpts5;
        VersionStrs when is_list(VersionStrs) ->
            F1 = fun (VStr) ->
                try
                    {true, list_to_existing_atom(VStr)}
                catch error:badarg ->
                    throw({bad_request, "invalid TLS version passed to "
                                        "/ldap/validate/simple-bind ssl_options.versions"})
                end
            end,
            Versions = lists:filtermap(F1, VersionStrs),
            [{versions, Versions} | TlsOpts5]
    end,
    TlsOpts7 = case maps:get(<<"ssl_hostname_verification">>, SslOptionsMap, undefined) of
        undefined ->
            TlsOpts6;
        "wildcard" ->
            [{customize_hostname_check, [{match_fun, public_key:pkix_verify_hostname_match_fun(https)}]} | TlsOpts6];
        _ ->
            throw({bad_request, "invalid value passed to "
                                "/ldap/validate/simple-bind ssl_options.ssl_hostname_verification"})
    end,
    {ok, TlsOpts7};
tls_options(_BodyMap) ->
    {ok, []}.

unicode_format(Arg) ->
    rabbit_data_coercion:to_utf8_binary(io_lib:format("~tp", [Arg])).

unicode_format(Format, Args) ->
    rabbit_data_coercion:to_utf8_binary(io_lib:format(Format, Args)).

format_password_for_logging(<<>>) ->
    "[empty]";
format_password_for_logging(Password) ->
    io_lib:format("[~p characters]", [string:length(Password)]).

safe_parse_int(Value, FieldName) ->
    try
        rabbit_mgmt_util:parse_int(Value)
    catch
        throw:{error, {not_integer, BadValue}} ->
            Msg = unicode_format("invalid value for ~s: expected integer, got ~tp",
                                [FieldName, BadValue]),
            throw({bad_request, Msg})
    end.

safe_parse_bool(Value, FieldName) ->
    try
        rabbit_mgmt_util:parse_bool(Value)
    catch
        throw:{error, {not_boolean, BadValue}} ->
            Msg = unicode_format("invalid value for ~s: expected boolean, got ~tp",
                                [FieldName, BadValue]),
            throw({bad_request, Msg})
    end.
