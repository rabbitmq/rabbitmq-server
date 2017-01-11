-module(rabbit_trust_store_storage).

-export([refresh_certs/2, is_whitelisted/1]).
-export([init/0, terminate/0]).

-include_lib("public_key/include/public_key.hrl").

-record(entry, {
    cert_id :: string(),
    provider :: module(),
    identifier :: tuple(),
    certificate :: #'OTPCertificate'{}
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
    case list_certs(Provider, Config, ProviderState) of
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
            case get_cert_data(CertId, Attributes, Provider, Config) of
                {ok, Cert} ->
                    Id = extract_unique_attributes(Cert),
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

delete_cert(CertId, Provider) ->
    ets:match_delete(table_name(), #entry{cert_id = CertId,
                                          provider = Provider,
                                          identifier = '_'}).

save_cert(CertId, Provider, Id, Cert) ->
    ets:insert(table_name(), #entry{cert_id = CertId,
                                    provider = Provider,
                                    identifier = Id,
                                    certificate = Cert}).

-spec list_certs(Provider, Config, ProviderState) -> no_change | {ok, [{CertId, Attributes}]}
      when Provider :: module(),
           Config :: list(),
           ProviderState :: term(),
           CertId :: term(),
           Attributes :: term().
list_certs(Provider, Config, ProviderState) ->
    Provider:list_certs(Config, ProviderState).

-spec get_cert_data(CertId, Attributes, Provider, Config)
      -> {ok, {Issuer, Serial}} | {error, term()}
      when CertId :: term(),
           Attributes :: term(),
           Provider :: module(),
           Config :: list(),
           Issuer :: public_key:issuer_name(),
           Serial :: integer().
get_cert_data(CertId, Attributes, Provider, Config) ->
    Provider:get_cert_data(CertId, Attributes, Config).

get_old_cert_ids(Provider) ->
    lists:append(ets:match(table_name(),
                           #entry{provider = Provider,
                                  cert_id = '$1',
                                  identifier = '_'})).

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

