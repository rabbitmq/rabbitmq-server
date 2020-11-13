-module(rabbit_trust_store_opera_com_provider).

-behaviour(rabbit_trust_store_certificate_provider).

-export([list_certs/1, list_certs/2, load_cert/3]).

%% This is an example implementation for
%% rabbit_trust_store_certificate_provider behaviour.
%% The module uses https://certs.opera.com/02/roots/ as a source of
%% CA certificates
%% The module can be used as an example when
%% implementing certificate provider for trust store plugin.


%% This function loads a list of certificates
list_certs(_Config) ->
    inets:start(),
    case httpc:request(get, {"https://certs.opera.com/02/roots/", []},
                       [], [{body_format, binary}]) of
        {ok, {{_,Code,_}, _Headers, Body}} when Code div 100 == 2 ->
            %% First link in directory listing is a parent dir link.
            {match, [_ParentDirLink | CertMatches]} =
                re:run(Body, "<td><a href=\"([^\"]*)\">",
                       [global, {capture, all_but_first, binary}]),

            CertNames = lists:append(CertMatches),
            %% certs.opera.com uses thumbprints for certificate file names
            %% so they should be unique (there is no need to add change time)
            {ok,
             [{CertName,
               [{name, CertName},
                 %% Url can be formed from CertName, so there is no
                 %% need for this attribute.
                 %% But we use it as an example for providers where CertName and
                 %% url are different.
                {url, <<"https://certs.opera.com/02/roots/", CertName/binary>>}]}
              || CertName <- CertNames],
              nostate};
        Other -> {error, {http_error, Other}}
    end.

%% Since we have no state for the provider,
%% we call the stateless version of this functions
list_certs(Config, _) -> list_certs(Config).

%% This function loads a certificate data using certifocate ID and attributes.
%% We use the url parameter in attributes.
%% Some providers can ignore attributes and use CertId instead
load_cert(_CertId, Attributes, _Config) ->
    Url = proplists:get_value(url, Attributes),
    case httpc:request(get, {rabbit_data_coercion:to_list(Url), []},
                       [], [{body_format, binary}]) of
        {ok, {{_,Code,_}, _Headers, Body}} when Code div 100 == 2 ->
            %% We assume that there is only one certificate per file.
            BodySingleLine = binary:replace(Body, <<"\n">>, <<>>, [global]),
            {match, [CertEncoded|_]} =
                re:run(BodySingleLine,
                       "<certificate-data>(.*)</certificate-data>",
                       [{capture, all_but_first, binary}, ungreedy]),
            [{'Certificate', Cert, not_encrypted}] =
                public_key:pem_decode(<<"-----BEGIN CERTIFICATE-----\n",
                                        CertEncoded/binary,
                                        "\n-----END CERTIFICATE-----\n">>),
            {ok, Cert};
        Other -> {error, {http_error, Other}}
    end.



