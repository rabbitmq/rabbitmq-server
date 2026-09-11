%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_peer_discovery_httpc_SUITE).

-compile(export_all).

-include_lib("eunit/include/eunit.hrl").

all() ->
    [
     redact_headers_redacts_sensitive_headers_case_insensitively,
     redact_headers_preserves_non_tuple_entries
    ].

redact_headers_redacts_sensitive_headers_case_insensitively(_Config) ->
    Headers = [{"X-Consul-Token", "token-value"},
               {"aUtHoRiZaTiOn", "******"},
               {"Content-Type", "application/json"}],
    Redacted = rabbit_peer_discovery_httpc:redact_headers(Headers),
    ?assertEqual("...", proplists:get_value("X-Consul-Token", Redacted)),
    ?assertEqual("...", proplists:get_value("aUtHoRiZaTiOn", Redacted)),
    ?assertEqual("application/json", proplists:get_value("Content-Type", Redacted)).

redact_headers_preserves_non_tuple_entries(_Config) ->
    Headers = [nodelay,
               {"Authorization", "******"}],
    Redacted = rabbit_peer_discovery_httpc:redact_headers(Headers),
    ?assertEqual([nodelay,
                  {"Authorization", "..."}],
                 Redacted).
