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
%% Copyright (c) 2007-2019 Pivotal Software, Inc.  All rights reserved.
%%

-module(rabbit_ssl).

-include_lib("public_key/include/public_key.hrl").

-export([peer_cert_issuer/1, peer_cert_subject/1, peer_cert_validity/1]).
-export([peer_cert_subject_items/2, peer_cert_auth_name/1]).

%%--------------------------------------------------------------------------

-export_type([certificate/0]).

-type certificate() :: rabbit_cert_info:certificate().

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

%% Return a string describing the certificate's validity.
peer_cert_validity(Cert) ->
    rabbit_cert_info:validity(Cert).

%% Extract a username from the certificate
-spec peer_cert_auth_name
        (certificate()) -> binary() | 'not_found' | 'unsafe'.

peer_cert_auth_name(Cert) ->
    {ok, Mode} = application:get_env(rabbit, ssl_cert_login_from),
    peer_cert_auth_name(Mode, Cert).

peer_cert_auth_name(distinguished_name, Cert) ->
    case auth_config_sane() of
        true  -> iolist_to_binary(peer_cert_subject(Cert));
        false -> unsafe
    end;

peer_cert_auth_name(common_name, Cert) ->
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
        V           -> rabbit_log:warning("SSL certificate authentication "
                                          "disabled, verify=~p~n", [V]),
                       false
    end.
