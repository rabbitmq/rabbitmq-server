%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_trust_store_certificate_provider).

-include_lib("public_key/include/public_key.hrl").

-callback list_certs(Config)
    -> no_change | {ok, [{CertId, Attributes}], ProviderState}
    when Config :: list(),
         ProviderState :: term(),
         CertId :: term(),
         Attributes :: list().

-callback list_certs(Config, ProviderState)
    -> no_change | {ok, [{CertId, Attributes}], ProviderState}
    when Config :: list(),
         ProviderState :: term(),
         CertId :: term(),
         Attributes :: list().

-callback load_cert(CertId, Attributes, Config)
    -> {ok, Cert} | {error, term()}
    when CertId :: term(),
         Attributes :: list(),
         Config :: list(),
         Cert :: public_key:der_encoded().
