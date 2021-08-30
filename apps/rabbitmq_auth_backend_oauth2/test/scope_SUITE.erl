%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%
-module(scope_SUITE).

-compile(export_all).

-include_lib("rabbit_common/include/rabbit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

all() ->
    [
        permission_all,
        permission_vhost,
        permission_resource,
        permission_topic
    ].

permission_all(_Config) ->
    WildcardScopeWrite = <<"write:*/*">>,
    WildcardScopeWriteTopic = <<"write:*/*/*">>,
    WildcardScopeRead = <<"read:*/*">>,
    WildcardScopeReadTopic = <<"read:*/*/*">>,
    WildcardScopeConfigure = <<"configure:*/*">>,
    WildcardScopeConfigureTopic = <<"configure:*/*/*">>,

    ReadScopes = [WildcardScopeRead, WildcardScopeReadTopic],
    WriteScopes = [WildcardScopeWrite, WildcardScopeWriteTopic],
    ConfigureScopes = [WildcardScopeConfigure, WildcardScopeConfigureTopic],

    ExampleVhosts = [<<"/">>, <<"foo">>, <<"*">>, <<"foo/bar">>, <<"юникод"/utf8>>],
    ExampleResources = [<<"foo">>, <<"foo/bar">>, <<"*">>, <<"*/*">>, <<"юникод"/utf8>>],


    [ vhost_allowed(<<"/">>, Scope) ||
        Scope <- ReadScopes ++ WriteScopes ++ ConfigureScopes ],
    [ vhost_allowed(<<"foo">>, Scope) ||
        Scope <- ReadScopes ++ WriteScopes ++ ConfigureScopes ],
    [ vhost_allowed(<<"*">>, Scope) ||
        Scope <- ReadScopes ++ WriteScopes ++ ConfigureScopes ],
    [ vhost_allowed(<<"foo/bar">>, Scope) ||
        Scope <- ReadScopes ++ WriteScopes ++ ConfigureScopes ],
    [ vhost_allowed(<<"юникод"/utf8>>, Scope) ||
        Scope <- ReadScopes ++ WriteScopes ++ ConfigureScopes ],

    [ read_allowed(Vhost, Resource, Scope) ||
        Scope <- ReadScopes,
        Vhost <- ExampleVhosts,
        Resource <- ExampleResources ],

    [ write_allowed(Vhost, Resource, Scope) ||
        Scope <- WriteScopes,
        Vhost <- ExampleVhosts,
        Resource <- ExampleResources ],

    [ configure_allowed(Vhost, Resource, Scope) ||
        Scope <- ConfigureScopes,
        Vhost <- ExampleVhosts,
        Resource <- ExampleResources ],

    [ read_refused(Vhost, Resource, Scope) ||
        Scope <- WriteScopes ++ ConfigureScopes,
        Vhost <- ExampleVhosts,
        Resource <- ExampleResources ],

    [ write_refused(Vhost, Resource, Scope) ||
        Scope <- ReadScopes ++ ConfigureScopes,
        Vhost <- ExampleVhosts,
        Resource <- ExampleResources ],

    [ configure_refused(Vhost, Resource, Scope) ||
        Scope <- WriteScopes ++ ReadScopes,
        Vhost <- ExampleVhosts,
        Resource <- ExampleResources ].

permission_vhost(_Config) ->
    FooScopeWrite = <<"write:foo/*">>,
    FooScopeWriteTopic = <<"write:foo/*/*">>,
    FooScopeRead = <<"read:foo/*">>,
    FooScopeReadTopic = <<"read:foo/*/*">>,
    FooScopeConfigure = <<"configure:foo/*">>,
    FooScopeConfigureTopic = <<"configure:foo/*/*">>,

    ComplexVHost = <<"foo/bar/*/">>,
    EncodedVhost = cow_qs:urlencode(ComplexVHost),

    EncodedScopeWrite = <<"write:", EncodedVhost/binary, "/*">>,
    EncodedScopeWriteTopic = <<"write:", EncodedVhost/binary, "/*/*">>,
    EncodedScopeRead = <<"read:", EncodedVhost/binary, "/*">>,
    EncodedScopeReadTopic = <<"read:", EncodedVhost/binary, "/*/*">>,
    EncodedScopeConfigure = <<"configure:", EncodedVhost/binary, "/*">>,
    EncodedScopeConfigureTopic = <<"configure:", EncodedVhost/binary, "/*/*">>,

    FooReadScopes = [FooScopeRead, FooScopeReadTopic],
    EncodedReadScopes = [EncodedScopeRead, EncodedScopeReadTopic],

    FooWriteScopes = [FooScopeWrite, FooScopeWriteTopic],
    EncodedWriteScopes = [EncodedScopeWrite, EncodedScopeWriteTopic],

    FooConfigureScopes = [FooScopeConfigure, FooScopeConfigureTopic],
    EncodedConfigureScopes = [EncodedScopeConfigure, EncodedScopeConfigureTopic],

    ExampleResources = [<<"foo">>, <<"foo/bar">>, <<"*">>, <<"*/*">>, <<"юникод"/utf8>>],

    Tags = [<<"tag:management">>, <<"tag:policymaker">>],

    [ vhost_allowed(<<"foo">>, Scope) ||
        Scope <- FooReadScopes ++ FooWriteScopes ++ FooConfigureScopes ],
    [ vhost_allowed(ComplexVHost, [Scope] ++ Tags) ||
        Scope <- EncodedReadScopes ++ EncodedWriteScopes ++ EncodedConfigureScopes ],

    [ read_allowed(<<"foo">>, Resource, [Scope] ++ Tags) ||
        Scope <- FooReadScopes,
        Resource <- ExampleResources ],

    [ write_allowed(<<"foo">>, Resource, [Scope] ++ Tags) ||
        Scope <- FooWriteScopes,
        Resource <- ExampleResources ],

    [ configure_allowed(<<"foo">>, Resource, [Scope] ++ Tags) ||
        Scope <- FooConfigureScopes,
        Resource <- ExampleResources ],

    [ read_refused(<<"foo">>, Resource, [Scope] ++ Tags) ||
        Scope <- FooWriteScopes ++ FooConfigureScopes ++
                 EncodedWriteScopes ++ EncodedConfigureScopes,
        Resource <- ExampleResources ],

    [ write_refused(<<"foo">>, Resource, [Scope] ++ Tags) ||
        Scope <- FooReadScopes ++ FooConfigureScopes ++
                 EncodedReadScopes ++ EncodedConfigureScopes,
        Resource <- ExampleResources ],

    [ configure_refused(<<"foo">>, Resource, [Scope] ++ Tags) ||
        Scope <- FooWriteScopes ++ FooReadScopes ++
                 EncodedWriteScopes ++ EncodedReadScopes,
        Resource <- ExampleResources ],

    [ read_allowed(ComplexVHost, Resource, Scope) ||
        Scope <- EncodedReadScopes,
        Resource <- ExampleResources ],

    [ write_allowed(ComplexVHost, Resource, Scope) ||
        Scope <- EncodedWriteScopes,
        Resource <- ExampleResources ],

    [ configure_allowed(ComplexVHost, Resource, Scope) ||
        Scope <- EncodedConfigureScopes,
        Resource <- ExampleResources ],

    [ read_refused(ComplexVHost, Resource, Scope) ||
        Scope <- EncodedWriteScopes ++ EncodedConfigureScopes ++
                 FooWriteScopes ++ FooConfigureScopes,
        Resource <- ExampleResources ],

    [ write_refused(ComplexVHost, Resource, Scope) ||
        Scope <- EncodedReadScopes ++ EncodedConfigureScopes ++
                 FooReadScopes ++ FooConfigureScopes,
        Resource <- ExampleResources ],

    [ configure_refused(ComplexVHost, Resource, Scope) ||
        Scope <- EncodedWriteScopes ++ EncodedReadScopes ++
                 FooWriteScopes ++ FooReadScopes,
        Resource <- ExampleResources ].

permission_resource(_Config) ->
    ComplexResource = <<"bar*/baz">>,
    EncodedResource = cow_qs:urlencode(ComplexResource),

    ScopeWrite = <<"write:*/", EncodedResource/binary>>,
    ScopeWriteTopic = <<"write:*/", EncodedResource/binary, "/*">>,
    ScopeRead = <<"read:*/", EncodedResource/binary>>,
    ScopeReadTopic = <<"read:*/", EncodedResource/binary, "/*">>,
    ScopeConfigure = <<"configure:*/", EncodedResource/binary>>,
    ScopeConfigureTopic = <<"configure:*/", EncodedResource/binary, "/*">>,

    ExampleVhosts = [<<"/">>, <<"foo">>, <<"*">>, <<"foo/bar">>, <<"юникод"/utf8>>],
    ExampleResources = [<<"foo">>, <<"foo/bar">>, <<"*">>, <<"*/*">>, <<"юникод"/utf8>>],

    %% Resource access is allowed for complex resource with any vhost
    [ read_allowed(Vhost, ComplexResource, Scope) ||
        Scope <- [ScopeRead, ScopeReadTopic],
        Vhost <- ExampleVhosts ],
    [ write_allowed(Vhost, ComplexResource, Scope) ||
        Scope <- [ScopeWrite, ScopeWriteTopic],
        Vhost <- ExampleVhosts ],
    [ configure_allowed(Vhost, ComplexResource, Scope) ||
        Scope <- [ScopeConfigure, ScopeConfigureTopic],
        Vhost <- ExampleVhosts ],

    %% Resource access is refused for any other resource
    [ read_refused(Vhost, Resource, Scope) ||
        Scope <- [ScopeWrite, ScopeWriteTopic,
                  ScopeRead, ScopeReadTopic,
                  ScopeConfigure, ScopeConfigureTopic],
        Vhost <- ExampleVhosts,
        Resource <- ExampleResources ],

    [ write_refused(Vhost, Resource, Scope) ||
        Scope <- [ScopeWrite, ScopeWriteTopic,
                  ScopeRead, ScopeReadTopic,
                  ScopeConfigure, ScopeConfigureTopic],
        Vhost <- ExampleVhosts,
        Resource <- ExampleResources ],

    [ configure_refused(Vhost, Resource, Scope) ||
        Scope <- [ScopeWrite, ScopeWriteTopic,
                  ScopeRead, ScopeReadTopic,
                  ScopeConfigure, ScopeConfigureTopic],
        Vhost <- ExampleVhosts,
        Resource <- ExampleResources ].

permission_topic(_Config) ->
    TopicWildcardRead = <<"read:*/*/*">>,
    TopicVhostRead = <<"read:vhost/*/*">>,
    TopicResourceRead = <<"read:*/exchange/*">>,
    TopicRoutingKeyRead = <<"read:*/*/rout">>,
    TopicRoutingSuffixKeyRead = <<"read:*/*/*rout">>,

    ExampleVhosts = [<<"/">>, <<"foo">>, <<"*">>, <<"foo/bar">>, <<"юникод"/utf8>>],
    ExampleResources = [<<"foo">>, <<"foo/bar">>, <<"*">>, <<"*/*">>, <<"юникод"/utf8>>],
    ExampleRoutingKeys = [<<"rout">>, <<"norout">>, <<"some_other">>],

    [ topic_read_allowed(VHost, Resource, RoutingKey, TopicWildcardRead) ||
        VHost <- ExampleVhosts,
        Resource <- ExampleResources,
        RoutingKey <- ExampleRoutingKeys],

    [ topic_read_allowed(<<"vhost">>, Resource, RoutingKey, TopicVhostRead) ||
        Resource <- ExampleResources,
        RoutingKey <- ExampleRoutingKeys],

    [ topic_read_allowed(VHost, <<"exchange">>, RoutingKey, TopicResourceRead) ||
        VHost <- ExampleVhosts,
        RoutingKey <- ExampleRoutingKeys],

    [ topic_read_allowed(VHost, Resource, <<"rout">>, TopicRoutingKeyRead) ||
        VHost <- ExampleVhosts,
        Resource <- ExampleResources],

    [ topic_read_allowed(VHost, Resource, RoutingKey, TopicRoutingSuffixKeyRead) ||
        VHost <- ExampleVhosts,
        Resource <- ExampleResources,
        RoutingKey <- [<<"rout">>, <<"norout">>, <<"sprout">>]],

    [ topic_read_refused(VHost, Resource, RoutingKey, TopicVhostRead) ||
        VHost <- ExampleVhosts,
        Resource <- ExampleResources,
        RoutingKey <- ExampleRoutingKeys],

    [ topic_read_refused(VHost, Resource, RoutingKey, TopicResourceRead) ||
        VHost <- ExampleVhosts,
        Resource <- ExampleResources,
        RoutingKey <- ExampleRoutingKeys],

    [ topic_read_refused(VHost, Resource, RoutingKey, TopicRoutingKeyRead) ||
        VHost <- ExampleVhosts,
        Resource <- ExampleResources,
        RoutingKey <- [<<"foo">>, <<"bar">>]].

vhost_allowed(Vhost, Scopes) when is_list(Scopes) ->
    ?assertEqual(true, rabbit_oauth2_scope:vhost_access(Vhost, Scopes));

vhost_allowed(Vhost, Scope) ->
    vhost_allowed(Vhost, [Scope]).

read_allowed(Vhost, Resource, Scopes) when is_list(Scopes) ->
    resource_perm(Vhost, Resource, Scopes, read, true);

read_allowed(Vhost, Resource, Scope) ->
    resource_perm(Vhost, Resource, Scope, read, true).

read_refused(Vhost, Resource, Scopes) when is_list(Scopes) ->
    resource_perm(Vhost, Resource, Scopes, read, false);

read_refused(Vhost, Resource, Scope) ->
    resource_perm(Vhost, Resource, Scope, read, false).

write_allowed(Vhost, Resource, Scopes) when is_list(Scopes) ->
    resource_perm(Vhost, Resource, Scopes, write, true);

write_allowed(Vhost, Resource, Scope) ->
    resource_perm(Vhost, Resource, Scope, write, true).

write_refused(Vhost, Resource, Scopes) when is_list(Scopes) ->
    resource_perm(Vhost, Resource, Scopes, write, false);

write_refused(Vhost, Resource, Scope) ->
    resource_perm(Vhost, Resource, Scope, write, false).

configure_allowed(Vhost, Resource, Scopes) when is_list(Scopes) ->
    resource_perm(Vhost, Resource, Scopes, configure, true);

configure_allowed(Vhost, Resource, Scope) ->
    resource_perm(Vhost, Resource, Scope, configure, true).

configure_refused(Vhost, Resource, Scopes) when is_list(Scopes) ->
    resource_perm(Vhost, Resource, Scopes, configure, false);

configure_refused(Vhost, Resource, Scope) ->
    resource_perm(Vhost, Resource, Scope, configure, false).


resource_perm(Vhost, Resource, Scopes, Permission, Result) when is_list(Scopes) ->
    [ ?assertEqual(Result, rabbit_oauth2_scope:resource_access(
          #resource{virtual_host = Vhost,
                    kind = Kind,
                    name = Resource},
          Permission,
          Scopes)) || Kind <- [queue, exchange] ];

resource_perm(Vhost, Resource, Scope, Permission, Result) ->
    resource_perm(Vhost, Resource, [Scope], Permission, Result).

topic_read_allowed(Vhost, Resource, RoutingKey, Scopes) when is_list(Scopes) ->
    topic_perm(Vhost, Resource, RoutingKey, Scopes, read, true);

topic_read_allowed(Vhost, Resource, RoutingKey, Scope) ->
    topic_perm(Vhost, Resource, RoutingKey, Scope, read, true).

topic_read_refused(Vhost, Resource, RoutingKey, Scopes) when is_list(Scopes) ->
    topic_perm(Vhost, Resource, RoutingKey, Scopes, read, false);

topic_read_refused(Vhost, Resource, RoutingKey, Scope) ->
    topic_perm(Vhost, Resource, RoutingKey, Scope, read, false).

topic_perm(Vhost, Resource, RoutingKey, Scopes, Permission, Result) when is_list(Scopes) ->
    ?assertEqual(Result, rabbit_oauth2_scope:topic_access(
        #resource{virtual_host = Vhost,
                  kind = topic,
                  name = Resource},
        Permission,
        #{routing_key => RoutingKey},
        Scopes));

topic_perm(Vhost, Resource, RoutingKey, Scope, Permission, Result) ->
    topic_perm(Vhost, Resource, RoutingKey, [Scope], Permission, Result).
