%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_federation_upstream).

-include("rabbit_federation.hrl").
-include_lib("rabbit/include/amqqueue.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

-export([federate/1, for/1, for/2, params_to_string/1, to_params/2]).
%% For testing
-export([from_set/2, from_pattern/2, remove_credentials/1]).

-import(rabbit_misc, [pget/2, pget/3]).
-import(rabbit_federation_util, [name/1, vhost/1, r/1]).
-import(rabbit_data_coercion, [to_atom/1]).

%%----------------------------------------------------------------------------

federate(XorQ) ->
    rabbit_policy:get(<<"federation-upstream">>, XorQ) =/= undefined orelse
        rabbit_policy:get(<<"federation-upstream-set">>, XorQ) =/= undefined orelse
            rabbit_policy:get(<<"federation-upstream-pattern">>, XorQ) =/= undefined.

for(XorQ) ->
    case federate(XorQ) of
        false -> [];
        true  -> from_set_contents(upstreams(XorQ), XorQ)
    end.

for(XorQ, UpstreamName) ->
    case federate(XorQ) of
        false -> [];
        true  -> rabbit_federation_util:find_upstreams(
                   UpstreamName, from_set_contents(upstreams(XorQ), XorQ))
    end.

upstreams(XorQ) ->
    UName = rabbit_policy:get(<<"federation-upstream">>, XorQ),
    USetName = rabbit_policy:get(<<"federation-upstream-set">>, XorQ),
    UPatternValue = rabbit_policy:get(<<"federation-upstream-pattern">>, XorQ),
    %% Cannot define 2 at a time, see rabbit_federation_parameters:validate_policy/1
    case {UName, USetName, UPatternValue} of
        {undefined, undefined, undefined} -> [];
        {undefined, undefined, _}         -> find_contents(UPatternValue, vhost(XorQ));
        {undefined, _, undefined}         -> set_contents(USetName, vhost(XorQ));
        {_,         undefined, undefined} -> [[{<<"upstream">>, UName}]]
    end.

params_table(SafeURI, XorQ) ->
    Key = case XorQ of
              #exchange{} -> <<"exchange">>;
              Q when ?is_amqqueue(Q) -> <<"queue">>
          end,
    [{<<"uri">>,          longstr, SafeURI},
     {Key,                longstr, name(XorQ)}].

params_to_string(#upstream_params{safe_uri = SafeURI,
                                  x_or_q   = XorQ}) ->
    print("~s on ~s", [rabbit_misc:rs(r(XorQ)), SafeURI]).

remove_credentials(URI) ->
    list_to_binary(amqp_uri:remove_credentials(binary_to_list(URI))).

to_params(Upstream = #upstream{uris = URIs}, XorQ) ->
    URI = lists:nth(rand:uniform(length(URIs)), URIs),
    {ok, Params} = amqp_uri:parse(binary_to_list(URI), vhost(XorQ)),
    XorQ1 = with_name(Upstream, vhost(Params), XorQ),
    SafeURI = remove_credentials(URI),
    #upstream_params{params   = Params,
                     uri      = URI,
                     x_or_q   = XorQ1,
                     safe_uri = SafeURI,
                     table    = params_table(SafeURI, XorQ)}.

print(Fmt, Args) -> iolist_to_binary(io_lib:format(Fmt, Args)).

from_set(SetName, XorQ) ->
    from_set_contents(set_contents(SetName, vhost(XorQ)), XorQ).

from_pattern(SetName, XorQ) ->
    from_set_contents(find_contents(SetName, vhost(XorQ)), XorQ).

set_contents(<<"all">>, VHost) ->
    Upstreams0 = rabbit_runtime_parameters:list(
                    VHost, <<"federation-upstream">>),
    Upstreams  = [rabbit_data_coercion:to_list(U) || U <- Upstreams0],
    [[{<<"upstream">>, pget(name, U)}] || U <- Upstreams];

set_contents(SetName, VHost) ->
    case rabbit_runtime_parameters:value(
           VHost, <<"federation-upstream-set">>, SetName) of
        not_found -> [];
        Set       -> Set
    end.

find_contents(RegExp, VHost) ->
    Upstreams0 = rabbit_runtime_parameters:list(
                    VHost, <<"federation-upstream">>),
    Upstreams  = [rabbit_data_coercion:to_list(U) || U <- Upstreams0,
                    re:run(pget(name, U), RegExp) =/= nomatch],
    [[{<<"upstream">>, pget(name, U)}] || U <- Upstreams].

from_set_contents(Set, XorQ) ->
    Results = [from_set_element(P, XorQ) || P <- Set],
    [R || R <- Results, R =/= not_found].

from_set_element(UpstreamSetElem0, XorQ) ->
    UpstreamSetElem = rabbit_data_coercion:to_proplist(UpstreamSetElem0),
    Name = bget(upstream, UpstreamSetElem, []),
    case rabbit_runtime_parameters:value(
           vhost(XorQ), <<"federation-upstream">>, Name) of
        not_found  -> not_found;
        Upstream   -> from_upstream_or_set(
                        UpstreamSetElem, Name, Upstream, XorQ)
    end.

from_upstream_or_set(US, Name, U, XorQ) ->
    URIParam = bget(uri, US, U),
    URIs = case URIParam of
               B when is_binary(B) -> [B];
               L when is_list(L)   -> L
           end,
    #upstream{uris            = URIs,
              exchange_name   = bget(exchange,          US, U, name(XorQ)),
              queue_name      = bget(queue,             US, U, name(XorQ)),
              consumer_tag    = bget('consumer-tag',    US, U, <<"federation-link-", Name/binary>>),
              prefetch_count  = bget('prefetch-count',  US, U, ?DEF_PREFETCH),
              reconnect_delay = bget('reconnect-delay', US, U, 5),
              max_hops        = bget('max-hops',        US, U, 1),
              expires         = bget(expires,           US, U, none),
              message_ttl     = bget('message-ttl',     US, U, none),
              trust_user_id   = bget('trust-user-id',   US, U, false),
              ack_mode        = to_atom(bget('ack-mode', US, U, <<"on-confirm">>)),
              ha_policy       = bget('ha-policy',       US, U, none),
              name            = Name,
              bind_nowait     = bget('bind-nowait',     US, U, false),
              resource_cleanup_mode = to_atom(bget('resource-cleanup-mode', US, U, <<"default">>)),
              channel_use_mode      = to_atom(bget('channel-use-mode', US, U, multiple))
    }.

%%----------------------------------------------------------------------------

bget(K, L1, L2) -> bget(K, L1, L2, undefined).

bget(K0, L1, L2, D) ->
    K   = a2b(K0),
    %% coerce maps to proplists
    PL1 = rabbit_data_coercion:to_list(L1),
    PL2 = rabbit_data_coercion:to_list(L2),
    case pget(K, PL1, undefined) of
        undefined -> pget(K, PL2, D);
        Result    -> Result
    end.

a2b(A) -> list_to_binary(atom_to_list(A)).

with_name(#upstream{exchange_name = XNameBin}, VHostBin, X = #exchange{}) ->
    X#exchange{name = rabbit_misc:r(VHostBin, exchange, XNameBin)};

with_name(#upstream{queue_name = QNameBin}, VHostBin, Q) when ?is_amqqueue(Q) ->
    amqqueue:set_name(Q, rabbit_misc:r(VHostBin, queue, QNameBin)).
