%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_ssl).

-include_lib("public_key/include/public_key.hrl").

-export([peer_cert_issuer/1, peer_cert_subject/1, peer_cert_validity/1]).
-export([peer_cert_subject_items/2, peer_cert_auth_name/1, peer_cert_auth_name/2]).
-export([cipher_suites_erlang/2, cipher_suites_erlang/1,
         cipher_suites_openssl/2, cipher_suites_openssl/1,
         cipher_suites/1]).
-export([info/2, cert_info/2]).
-export([wrap_password_opt/1]).

%%--------------------------------------------------------------------------

-export_type([certificate/0, ssl_cert_login_type/0]).

% Due to API differences between OTP releases.
-dialyzer(no_missing_calls).
-ignore_xref([{ssl_cipher_format, suite_legacy, 1},
              {ssl_cipher_format, suite, 1},
              {ssl_cipher_format, suite_to_str, 1},
              {ssl_cipher_format, erl_suite_definition, 1},
              {ssl_cipher_format, suite_map_to_openssl_str, 1},
              {ssl_cipher_format, suite_map_to_bin, 1}]).

-dialyzer({nowarn_function, peer_cert_auth_name/2}).

-type certificate() :: rabbit_cert_info:certificate().

-type cipher_suites_mode() :: default | all | anonymous.
-type tls_opts() :: [ssl:tls_server_option()] | [ssl:tls_client_option()].

-spec wrap_password_opt(tls_opts()) -> tls_opts().
wrap_password_opt(Opts0) ->
    rabbit_ssl_options:wrap_password_opt(Opts0).

-spec cipher_suites(cipher_suites_mode()) -> ssl:ciphers().
cipher_suites(Mode) ->
    Version = get_highest_protocol_version(),
    ssl:cipher_suites(Mode, Version).

-spec cipher_suites_erlang(cipher_suites_mode()) ->
    [ssl:old_cipher_suite()].
cipher_suites_erlang(Mode) ->
    Version = get_highest_protocol_version(),
    cipher_suites_erlang(Mode, Version).

-spec cipher_suites_erlang(cipher_suites_mode(),
                           ssl:protocol_version() | tls_record:tls_version()) ->
    [ssl:old_cipher_suite()].
cipher_suites_erlang(Mode, Version) ->
    [ format_cipher_erlang(C)
      || C <- ssl:cipher_suites(Mode, Version) ].

-spec cipher_suites_openssl(cipher_suites_mode()) ->
    [ssl:old_cipher_suite()].
cipher_suites_openssl(Mode) ->
    Version = get_highest_protocol_version(),
    cipher_suites_openssl(Mode, Version).

-spec cipher_suites_openssl(cipher_suites_mode(),
                           ssl:protocol_version() | tls_record:tls_version()) ->
    [ssl:old_cipher_suite()].
cipher_suites_openssl(Mode, Version) ->
    lists:filtermap(fun(C) ->
        OpenSSL = format_cipher_openssl(C),
        case is_list(OpenSSL) of
            true  -> {true, OpenSSL};
            false -> false
        end
    end,
    ssl:cipher_suites(Mode, Version)).


format_cipher_erlang(Cipher) ->
  ssl_cipher_format:suite_legacy(ssl_cipher_format:suite_map_to_bin(Cipher)).

format_cipher_openssl(Cipher) ->
    ssl_cipher_format:suite_map_to_openssl_str(Cipher).

-spec get_highest_protocol_version() -> tls_record:tls_atom_version().
get_highest_protocol_version() ->
    tls_record:protocol_version(
      tls_record:highest_protocol_version([])).

%%--------------------------------------------------------------------------
%% High-level functions used by reader
%%--------------------------------------------------------------------------

%% Return a string describing the certificate's issuer.
peer_cert_issuer(Cert) ->
    rabbit_cert_info:issuer(Cert).

%% Return a string describing the certificate's subject, as per RFC4514.
peer_cert_subject(Cert) ->
    rabbit_cert_info:subject(Cert).

%% Return the parts of the certificate's subject.
peer_cert_subject_items(Cert, Type) ->
    rabbit_cert_info:subject_items(Cert, Type).

%% Filters certificate SAN extensions by (OTP) SAN type name.
peer_cert_subject_alternative_names(Cert, Type) ->
    SANs = rabbit_cert_info:subject_alternative_names(Cert),
    lists:filter(fun({Key, _}) -> Key =:= Type end, SANs).

%% Return a string describing the certificate's validity.
peer_cert_validity(Cert) ->
    rabbit_cert_info:validity(Cert).

-type ssl_cert_login_type() ::
    {subject_alternative_name | subject_alt_name, atom(), integer()} |
    {distinguished_name | common_name, undefined, undefined }.

-spec extract_ssl_cert_login_settings() -> none | ssl_cert_login_type().
extract_ssl_cert_login_settings() ->
    case application:get_env(rabbit, ssl_cert_login_from) of
        {ok, Mode} ->
            case Mode of
                subject_alternative_name -> extract_san_login_type(Mode);
                subject_alt_name -> extract_san_login_type(Mode);
                _ -> {Mode, undefined, undefined}
            end;
        undefined -> none
    end.

extract_san_login_type(Mode) ->
    {Mode,
        application:get_env(rabbit, ssl_cert_login_san_type, dns),
        application:get_env(rabbit, ssl_cert_login_san_index, 0)
    }.

%% Extract a username from the certificate
-spec peer_cert_auth_name(certificate()) -> binary() | 'not_found' | 'unsafe'.
peer_cert_auth_name(Cert) ->
    case extract_ssl_cert_login_settings() of
        none -> 'not_found';
        Settings -> peer_cert_auth_name(Settings, Cert)
    end.

-spec peer_cert_auth_name(ssl_cert_login_type(), certificate()) -> binary() | 'not_found' | 'unsafe'.
peer_cert_auth_name({distinguished_name, _, _}, Cert) ->
    case auth_config_sane() of
        true  -> iolist_to_binary(peer_cert_subject(Cert));
        false -> unsafe
    end;

peer_cert_auth_name({subject_alt_name, Type, Index0}, Cert) ->
    peer_cert_auth_name({subject_alternative_name, Type, Index0}, Cert);

peer_cert_auth_name({subject_alternative_name, Type, Index0}, Cert) ->
    case auth_config_sane() of
        true  ->
            %% lists:nth/2 is 1-based
            Index  = Index0 + 1,
            OfType = peer_cert_subject_alternative_names(Cert, otp_san_type(Type)),
            rabbit_log:debug("Peer certificate SANs of type ~ts: ~tp, index to use with lists:nth/2: ~b", [Type, OfType, Index]),
            case length(OfType) of
                0                 -> not_found;
                N when N < Index  -> not_found;
                N when N >= Index ->
                    Nth = lists:nth(Index, OfType),
                    case Nth of
                      %% this is SAN of type otherName; it can be anything, so we simply try to extract the value
                      %% the best we can and return it. There aren't really any conventions or widely held expectations
                      %% about the format :(
                      {otherName, {'AnotherName', _, Value}} ->
                          rabbit_cert_info:sanitize_other_name(rabbit_data_coercion:to_binary(Value));
                      %% most SAN types return a pair: DNS, email, URI
                      {_, Value} ->
                          rabbit_data_coercion:to_binary(Value)
                    end
            end;
        false -> unsafe
    end;

peer_cert_auth_name({common_name, _, _}, Cert) ->
    %% If there is more than one CN then we join them with "," in a
    %% vaguely DN-like way. But this is more just so we do something
    %% more intelligent than crashing, if you actually want to escape
    %% things properly etc, use DN mode.
    case auth_config_sane() of
        true  -> case peer_cert_subject_items(Cert, ?'id-at-commonName') of
                     not_found -> not_found;
                     CNs       -> list_to_binary(string:join(CNs, ","))
                 end;
        false -> unsafe
    end.

auth_config_sane() ->
    {ok, Opts} = application:get_env(rabbit, ssl_options),
    case proplists:get_value(verify, Opts) of
        verify_peer -> true;
        V           -> rabbit_log:warning("TLS peer verification (authentication) is "
                                          "disabled, ssl_options.verify value used: ~tp. "
                                          "See https://www.rabbitmq.com/docs/ssl#peer-verification to learn more.", [V]),
                       false
    end.

otp_san_type(dns)        -> dNSName;
otp_san_type(ip)         -> iPAddress;
otp_san_type(email)      -> rfc822Name;
otp_san_type(uri)        -> uniformResourceIdentifier;
otp_san_type(other_name) -> otherName;
otp_san_type(Other)      -> Other.

info(ssl_protocol,     Socks) -> info0(fun ({P,         _}) -> P end, Socks);
info(ssl_key_exchange, Socks) -> info0(fun ({_, {K, _, _}}) -> K end, Socks);
info(ssl_cipher,       Socks) -> info0(fun ({_, {_, C, _}}) -> C end, Socks);
info(ssl_hash,         Socks) -> info0(fun ({_, {_, _, H}}) -> H end, Socks);
info(ssl, {Sock, ProxySock})  -> rabbit_net:proxy_ssl_info(Sock, ProxySock) /= nossl.

info0(F, {Sock, ProxySock}) ->
    case rabbit_net:proxy_ssl_info(Sock, ProxySock) of
        nossl       -> '';
        {error, _}  -> '';
        {ok, Items} ->
            P = proplists:get_value(protocol, Items),
            #{cipher := C,
              key_exchange := K,
              mac := H} = proplists:get_value(selected_cipher_suite, Items),
            F({P, {K, C, H}})
    end.

cert_info(peer_cert_issuer, Sock) ->
    cert_info0(fun peer_cert_issuer/1, Sock);
cert_info(peer_cert_subject, Sock) ->
    cert_info0(fun peer_cert_subject/1, Sock);
cert_info(peer_cert_validity, Sock) ->
    cert_info0(fun peer_cert_validity/1, Sock).

cert_info0(F, Sock) ->
    case rabbit_net:peercert(Sock) of
        nossl      -> '';
        {error, _} -> '';
        {ok, Cert} -> list_to_binary(F(Cert))
    end.
