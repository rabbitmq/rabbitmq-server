-module(rabbit_trust_store_storage).

-export([refresh_certs/2, is_whitelisted/1]).
-export([init/0, terminate/0, list/0]).

-include_lib("public_key/include/public_key.hrl").
-include_lib("stdlib/include/ms_transform.hrl").

-record(entry, {
    cert_id :: string(),
    provider :: module(),
    identifier :: tuple(),
    certificate :: public_key:der_encoded()
}).

init() ->
    ets:new(table_name(), table_options()).

terminate() ->
    true = ets:delete(table_name()).

-spec is_whitelisted(#'OTPCertificate'{}) -> boolean().
is_whitelisted(Cert) ->
    Id = extract_unique_attributes(Cert),
    ets:member(table_name(), Id).

-spec refresh_certs(Config, State) -> State
      when State :: [{module(), term()}],
           Config :: list().
refresh_certs(Config, State) ->
    Providers = get_providers(Config),
    lists:foldl(
        fun(Provider, NewStates) ->
            ProviderState = proplists:get_value(Provider, State, nostate),
            RefreshedState = refresh_provider_certs(Provider, Config, ProviderState),
            [{Provider, RefreshedState} | NewStates]
        end,
        [],
        Providers).

-spec refresh_provider_certs(Provider, Config, ProviderState) -> ProviderState
      when Provider :: module(),
           Config :: list(),
           ProviderState :: term().
refresh_provider_certs(Provider, Config, ProviderState) ->
    case Provider:list_certs(Config, ProviderState) of
        no_change ->
            ProviderState;
        {ok, CertsList, NewProviderState} ->
            update_certs(CertsList, Provider, Config),
            NewProviderState
    end.

update_certs(CertsList, Provider, Config) ->
    OldCertIds = get_old_cert_ids(Provider),
    {NewCertIds, _} = lists:unzip(CertsList),

    lists:foreach(
        fun(CertId) ->
            Attributes = proplists:get_value(CertId, CertsList),
            case get_cert_data(Provider, CertId, Attributes, Config) of
                {ok, Cert, Id} ->
                    save_cert(CertId, Provider, Id, Cert);
                {error, Reason} ->
                    rabbit_log:error("Unable to load CA sertificate ~p"
                                     " with provider ~p"
                                     " reason: ~p",
                                     [CertId, Provider, Reason])
            end
        end,
        NewCertIds -- OldCertIds),
    lists:foreach(
        fun(CertId) ->
            delete_cert(CertId, Provider)
        end,
        OldCertIds -- NewCertIds),
    ok.

get_cert_data(Provider, CertId, Attributes, Config) ->
    try
        case Provider:get_cert_data(CertId, Attributes, Config) of
            {ok, Cert} ->
                DecodedCert = public_key:pkix_decode_cert(Cert, otp),
                Id = extract_unique_attributes(DecodedCert),
                {ok, Cert, Id};
            {error, Reason} -> {error, Reason}
        end
    catch _:Error ->
        {error, Error}
    end.

delete_cert(CertId, Provider) ->
    MS = ets:fun2ms(fun(#entry{cert_id = CId, provider = P})
                    when P == Provider, CId == CertId ->
                        true
                    end),
    ets:select_delete(table_name(), MS).

save_cert(CertId, Provider, Id, Cert) ->
    ets:insert(table_name(), #entry{cert_id = CertId,
                                    provider = Provider,
                                    identifier = Id,
                                    certificate = Cert}).

get_old_cert_ids(Provider) ->
    MS = ets:fun2ms(fun(#entry{provider = P, cert_id = CId})
                    when P == Provider ->
                        CId
                    end),
    lists:append(ets:select(table_name(), MS)).

get_providers(Config) ->
    proplists:get_value(providers, Config, []).

table_name() ->
    trust_store_whitelist.

table_options() ->
    [protected,
     named_table,
     set,
     {keypos, #entry.identifier},
     {heir, none}].

extract_unique_attributes(#'OTPCertificate'{} = C) ->
    {Serial, Issuer} = case public_key:pkix_issuer_id(C, other) of
        {error, _Reason} ->
            {ok, Identifier} = public_key:pkix_issuer_id(C, self),
            Identifier;
        {ok, Identifier} ->
            Identifier
    end,
    {Issuer, Serial}.

list() ->
    Formatted = lists:map(
        fun(#entry{cert_id = CertId,
                provider = Provider,
                certificate = Cert,
                identifier = {_, Serial}}) ->
            Name = Provider:format_cert_id(CertId),
            Validity = rabbit_ssl:peer_cert_validity(Cert),
            Subject = rabbit_ssl:peer_cert_subject(Cert),
            Issuer = rabbit_ssl:peer_cert_issuer(Cert),
            Text = io_lib:format("Name: ~s~nSerial: ~p | 0x~.16.0B~n"
                                 "Subject: ~s~nIssuer: ~s~nValidity: ~p~n",
                                 [Name, Serial, Serial,
                                  Subject, Issuer, Validity]),
            lists:flatten(Text)
        end,
        ets:tab2list(table_name())),
    string:join(Formatted, "~n~n").
