%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2026 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(unit_rabbit_guid_SUITE).

-include_lib("eunit/include/eunit.hrl").

-compile([nowarn_export_all, export_all]).

all() ->
    [secure_guid_randomness,
     secure_guid_random_failure,
     secure_guid_uuid_format].

init_per_testcase(Testcase, Config)
  when Testcase =:= secure_guid_randomness;
       Testcase =:= secure_guid_random_failure ->
    put(guid_secure, {{0, node(), make_ref()}, 0}),
    ok = meck:new(crypto, [unstick, no_link, passthrough]),
    Config;
init_per_testcase(_Testcase, Config) ->
    Config.

end_per_testcase(Testcase, _Config)
  when Testcase =:= secure_guid_randomness;
       Testcase =:= secure_guid_random_failure ->
    ok = meck:unload(crypto),
    erase(guid_secure),
    ok;
end_per_testcase(_Testcase, _Config) ->
    ok.

secure_guid_randomness(_Config) ->
    ok = meck:expect(crypto, strong_rand_bytes, [16],
                     meck:seq([<<16#00112233445566778899aabbccddeeff:128>>,
                               <<16#ffeeddccbbaa99887766554433221100:128>>])),
    ?assertEqual(<<16#00112233445546778899aabbccddeeff:128>>,
                 rabbit_guid:gen_secure()),
    ?assertEqual(<<16#ffeeddccbbaa4988b766554433221100:128>>,
                 rabbit_guid:gen_secure()),
    ?assertEqual(2, meck:num_calls(crypto, strong_rand_bytes, [16])),
    ?assert(meck:validate(crypto)).

secure_guid_random_failure(_Config) ->
    ok = meck:expect(crypto, strong_rand_bytes,
                     fun(16) -> meck:exception(error, low_entropy) end),
    ?assertError(low_entropy, rabbit_guid:gen_secure()),
    ?assert(meck:validate(crypto)).

secure_guid_uuid_format(_Config) ->
    Guid = rabbit_guid:gen_secure(),
    ?assertMatch(<<_:48, 4:4, _:12, 2:2, _:62>>, Guid),
    Text = rabbit_guid:to_string(Guid),
    ?assertEqual(36, length(Text)),
    ?assertEqual(match,
                 re:run(Text,
                        "^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$",
                        [{capture, none}])),
    <<"amq.gen-", Encoded/binary>> = rabbit_guid:binary(Guid, "amq.gen"),
    ?assertEqual(22, byte_size(Encoded)).
