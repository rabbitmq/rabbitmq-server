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
    {[<<"HEAD">>, <<"PUT">>, <<"OPTIONS">>], ReqData, Context}.

resource_exists(ReqData, Context) ->
    {true, ReqData, Context}.

is_authorized(ReqData, Context) ->
    rabbit_mgmt_util:is_authorized(ReqData, Context).

accept_content(ReqData0, Context) ->
    F = fun (_Values, BodyMap, ReqData1) ->
        Port = rabbit_mgmt_util:parse_int(maps:get(port, BodyMap, 389)),
        UseSsl = rabbit_mgmt_util:parse_bool(maps:get(use_ssl, BodyMap, false)),
        UseStartTls = rabbit_mgmt_util:parse_bool(maps:get(use_starttls, BodyMap, false)),
        Servers = maps:get(servers, BodyMap, []),
        UserDN = maps:get(user_dn, BodyMap, <<"">>),
        Password = maps:get(password, BodyMap, <<"">>),
        Options0 = [
            {port, Port},
            {timeout, 5000}
        ],
        try
            {ok, Options1} = maybe_add_ssl_options(Options0, UseSsl, BodyMap),
            case eldap:open(Servers, Options1) of
                {ok, LDAP} ->
                    Result = case maybe_starttls(LDAP, UseStartTls, BodyMap) of
                        ok ->
                            case eldap:simple_bind(LDAP, UserDN, Password) of
                                ok ->
                                    {true, ReqData1, Context};
                                {error, invalidCredentials} ->
                                    rabbit_mgmt_util:not_authorised("invalid credentials", ReqData1, Context);
                                {error, unwillingToPerform} ->
                                    rabbit_mgmt_util:not_authorised("invalid credentials", ReqData1, Context);
                                {error, E} ->
                                    Reason = unicode_format(E),
                                    rabbit_mgmt_util:bad_request(Reason, ReqData1, Context)
                            end;
                        Error ->
                            Reason = unicode_format(Error),
                            rabbit_mgmt_util:bad_request(Reason, ReqData1, Context)
                    end,
                    eldap:close(LDAP),
                    Result;
                {error, E} ->
                    Reason = unicode_format(E),
                    rabbit_mgmt_util:bad_request(Reason, ReqData1, Context)
            end
        catch throw:{bad_request, ErrMsg} ->
            rabbit_mgmt_util:bad_request(ErrMsg, ReqData1, Context)
        end
    end,
    rabbit_mgmt_util:with_decode([], ReqData0, Context, F).

%%--------------------------------------------------------------------

unicode_format(Arg) ->
    rabbit_data_coercion:to_utf8_binary(io_lib:format("~tp", [Arg])).

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

tls_options(BodyMap) ->
    case maps:get(ssl_options, BodyMap, undefined) of
        undefined ->
            {ok, []};
        SslOptionsMap ->
            %% NB: for some reason the "cacertfile" key isn't turned into an atom
            TlsOpts0 = case maps:get(<<"cacertfile">>, SslOptionsMap, undefined) of
                undefined ->
                    [];
                CaCertfile ->
                    [{cacertfile, CaCertfile}]
            end,
            TlsOpts1 = case maps:get(<<"cacert_pem_data">>, SslOptionsMap, undefined) of
                undefined ->
                    TlsOpts0;
                CaCertPems when is_list(CaCertPems) ->
                    F0 = fun (P) ->
                        case public_key:pem_decode(P) of
                            [{'Certificate', CaCertDerEncoded, not_encrypted}] ->
                                {true, CaCertDerEncoded};
                            _Unexpected ->
                                throw({bad_request, "unexpected cacert_pem_data passed to "
                                                    "/ldap/validate/simple-bind ssl_options.cacerts"})
                        end
                    end,
                    CaCertsDerEncoded = lists:filtermap(F0, CaCertPems),
                    [{cacerts, CaCertsDerEncoded} | TlsOpts0];
                _ ->
                    TlsOpts0
            end,
            TlsOpts2 = case maps:get(<<"verify">>, SslOptionsMap, undefined) of
                undefined ->
                    TlsOpts1;
                Verify ->
                    VerifyStr = unicode:characters_to_list(Verify),
                    [{verify, list_to_existing_atom(VerifyStr)} | TlsOpts1]
            end,
            TlsOpts3 = case maps:get(<<"server_name_indication">>, SslOptionsMap, disable) of
                disable ->
                    TlsOpts2;
                SniValue ->
                    SniStr = unicode:characters_to_list(SniValue),
                    [{server_name_indication, SniStr} | TlsOpts2]
            end,
            TlsOpts4 = case maps:get(<<"depth">>, SslOptionsMap, undefined) of
                undefined ->
                    TlsOpts3;
                DepthValue ->
                    Depth = rabbit_data_coercion:to_integer(DepthValue),
                    [{depth, Depth} | TlsOpts3]
            end,
            TlsOpts5 = case maps:get(<<"versions">>, SslOptionsMap, undefined) of
                undefined ->
                    TlsOpts4;
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
                    [{versions, Versions} | TlsOpts4]
            end,
            {ok, TlsOpts5}
    end.
