%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_ldap_seed).

-include_lib("eldap/include/eldap.hrl").

-export([seed/1,delete/1]).

seed(Logon) ->
    H = connect(Logon),
    ok = add(H, rabbitmq_com()),
    ok = add(H, ou("people")),
    [ add(H, P) || P <- people() ],
    ok = add(H, ou("vhosts")),
    ok = add(H, test()),
    ok = add(H, ou("groups")),
    [ add(H, P) || P <- groups() ],
    eldap:close(H),
    ok.

rabbitmq_com() ->
    {"dc=rabbitmq,dc=com",
      [{"objectClass", ["dcObject", "organization"]},
       {"dc", ["rabbitmq"]},
       {"o", ["Test"]}]}.


delete(Logon) ->
    H = connect(Logon),
    eldap:delete(H, "ou=test,dc=rabbitmq,dc=com"),
    eldap:delete(H, "ou=test,ou=vhosts,dc=rabbitmq,dc=com"),
    eldap:delete(H, "ou=vhosts,dc=rabbitmq,dc=com"),
    [ eldap:delete(H, P) || {P, _} <- groups() ],
    [ eldap:delete(H, P) || {P, _} <- people() ],
    eldap:delete(H, "ou=groups,dc=rabbitmq,dc=com"),
    eldap:delete(H, "ou=people,dc=rabbitmq,dc=com"),
    eldap:delete(H, "dc=rabbitmq,dc=com"),
    eldap:close(H),
    ok.

people() ->
    [ bob(),
      dominic(),
      charlie(),
      edward(),
      johndoe(),
      alice(),
      peter(),
      carol(),
      jimmy()
    ].

groups() ->
    [wheel_group(),
     people_group(),
     staff_group(),
     bobs_group(),
     bobs2_group(),
     admins_group()
    ].

wheel_group() ->
    {A, _} = alice(),
    {C, _} = charlie(),
    {D, _} = dominic(),
    {P, _} = peter(),
    {"cn=wheel,ou=groups,dc=rabbitmq,dc=com",
     [{"objectClass", ["groupOfNames"]},
      {"cn", ["wheel"]},
      {"member", [A, C, D, P]}]}.

people_group() ->
    {C, _} = charlie(),
    {D, _} = dominic(),
    {P, _} = peter(),
    {"cn=people,ou=groups,dc=rabbitmq,dc=com",
     [{"objectClass", ["groupOfNames"]},
      {"cn", ["people"]},
      {"member", [C, D, P]}]}.

staff_group() ->
    {C, _} = charlie(),
    {D, _} = dominic(),
    {P, _} = peter(),
    {"cn=staff,ou=groups,dc=rabbitmq,dc=com",
     [{"objectClass", ["groupOfNames"]},
      {"cn", ["people"]},
      {"member", [C, D, P]}]}.

bobs_group() ->
    {B, _} = bob(),
    {"cn=bobs,ou=groups,dc=rabbitmq,dc=com",
     [{"objectClass", ["groupOfNames"]},
      {"cn", ["bobs"]},
      {"member", [B]}]}.

bobs2_group() ->
    {B, _} = bobs_group(),
    {"cn=bobs2,ou=groups,dc=rabbitmq,dc=com",
     [{"objectClass", ["groupOfNames"]},
      {"cn", ["bobs2"]},
      {"member", [B]}]}.

admins_group() ->
    {B, _} = bobs2_group(),
    {W, _} = wheel_group(),
    {"cn=admins,ou=groups,dc=rabbitmq,dc=com",
     [{"objectClass", ["groupOfNames"]},
      {"cn", ["admins"]},
      {"member", [B, W]}]}.

person(Cn, Sn) ->
    {"cn="++Cn++",ou=people,dc=rabbitmq,dc=com",
     [{"objectClass", ["person"]},
      {"cn", [Cn]},
      {"sn", [Sn]},
      {"userPassword", ["password"]}]}.

bob() -> person("Bob", "Robert").
dominic() -> person("Dominic", "Dom").
charlie() -> person("Charlie", "Charlie Boy").
edward() -> person("Edward", "Ed").
johndoe() -> person("John Doe", "Doe").

alice() ->
    {"cn=Alice,ou=people,dc=rabbitmq,dc=com",
     [{"objectClass", ["person"]},
      {"cn", ["Alice"]},
      {"sn", ["Ali"]},
      {"userPassword", ["password"]},
      {"description", ["can-declare-queues"]}]}.

peter() ->
    {"uid=peter,ou=people,dc=rabbitmq,dc=com",
     [{"cn", ["Peter"]},
      {"givenName", ["Peter"]},
      {"sn", ["Jones"]},
      {"uid", ["peter"]},
      {"uidNumber", ["5000"]},
      {"gidNumber", ["10000"]},
      {"homeDirectory", ["/home/peter"]},
      {"mail", ["peter.jones@rabbitmq.com"]},
      {"objectClass", ["top",
                       "posixAccount",
                       "shadowAccount",
                       "inetOrgPerson",
                       "organizationalPerson",
                       "person"]},
      {"loginShell", ["/bin/bash"]},
      {"userPassword", ["password"]},
      {"memberOf", ["cn=wheel,ou=groups,dc=rabbitmq,dc=com",
                    "cn=staff,ou=groups,dc=rabbitmq,dc=com",
                    "cn=people,ou=groups,dc=rabbitmq,dc=com"]}]}.

carol() ->
    {"uid=carol,ou=people,dc=rabbitmq,dc=com",
     [{"cn", ["Carol"]},
      {"givenName", ["Carol"]},
      {"sn", ["Meyers"]},
      {"uid", ["peter"]},
      {"uidNumber", ["655"]},
      {"gidNumber", ["10000"]},
      {"homeDirectory", ["/home/carol"]},
      {"mail", ["carol.meyers@example.com"]},
      {"objectClass", ["top",
                       "posixAccount",
                       "shadowAccount",
                       "inetOrgPerson",
                       "organizationalPerson",
                       "person"]},
      {"loginShell", ["/bin/bash"]},
      {"userPassword", ["password"]}]}.

% rabbitmq/rabbitmq-auth-backend-ldap#100
jimmy() ->
    {"cn=Jimmy,ou=people,dc=rabbitmq,dc=com",
     [{"objectClass", ["person"]},
      {"cn", ["Jimmy"]},
      {"sn", ["Makes"]},
      {"userPassword", ["password"]},
      {"description", ["^RMQ-foobar", "^RMQ-.*$"]}]}.

add(H, {A, B}) ->
    ok = eldap:add(H, A, B).

connect({Host, Port}) ->
    {ok, H} = eldap:open([Host], [{port, Port}]),
    ok = eldap:simple_bind(H, "cn=admin,dc=rabbitmq,dc=com", "admin"),
    H.

ou(Name) ->
    {"ou=" ++ Name ++ ",dc=rabbitmq,dc=com", [{"objectClass", ["organizationalUnit"]}, {"ou", [Name]}]}.

test() ->
    {"ou=test,ou=vhosts,dc=rabbitmq,dc=com", [{"objectClass", ["top", "organizationalUnit"]}, {"ou", ["test"]}]}.

