%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_direct_reply_to).

%% API
-export([
    compute_key_and_suffix_v1/1,
    decode_reply_to_v1/1
]).

%%
%% API
%%

-spec compute_key_and_suffix_v1(pid()) -> {binary(), binary()}.
compute_key_and_suffix_v1(Pid) ->
    Key = base64:encode(rabbit_guid:gen_secure()),
    PidEnc = base64:encode(term_to_binary(Pid)),
    Suffix = <<PidEnc/binary, ".", Key/binary>>,
    {Key, Suffix}.

-spec decode_reply_to_v1(binary()) -> {ok, pid(), binary()} | error.
decode_reply_to_v1(Rest) ->
    case string:tokens(binary_to_list(Rest), ".") of
        [PidEnc, Key] -> Pid = binary_to_term(base64:decode(PidEnc)),
                         {ok, Pid, Key};
        _             -> error
    end.
