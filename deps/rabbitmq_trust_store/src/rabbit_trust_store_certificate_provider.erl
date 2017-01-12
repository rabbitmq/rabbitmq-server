-module(rabbit_trust_store_certificate_provider).

-include_lib("public_key/include/public_key.hrl").

-callback list_certs(Config, ProviderState)
    -> no_change | {ok, [{CertId, Attributes}]}
    when Config :: list(),
         ProviderState :: term(),
         CertId :: term(),
         Attributes :: term().

-callback get_cert_data(CertId, Attributes, Config)
    -> {ok, Cert} | {error, term()}
    when CertId :: term(),
         Attributes :: term(),
         Config :: list(),
         Cert :: public_key:der_encoded().


-callback format_cert_id(CertId) -> binary()
    when CertId :: term().