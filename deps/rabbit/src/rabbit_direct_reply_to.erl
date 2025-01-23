%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_direct_reply_to).

-export([compute_key_and_suffix/1,
         decode_reply_to/2]).

%% This pid encoding function produces values that are of mostly fixed size
%% regardless of the node name length.
-spec compute_key_and_suffix(pid()) ->
    {binary(), binary()}.
compute_key_and_suffix(Pid) ->
    Key = base64:encode(rabbit_guid:gen()),

    PidParts0 = #{node := Node} = pid_recomposition:decompose(Pid),
    %% Note: we hash the entire node name. This is sufficient for our needs of shortening node name
    %% in the TTB-encoded pid, and helps avoid doing the node name split for every single cluster member
    %% in rabbit_nodes:all_running_with_hashes/0.
    %%
    %% We also use a synthetic node prefix because the hash alone will be sufficient to
    NodeHash = erlang:phash2(Node),
    PidParts = maps:update(node, rabbit_nodes_common:make("reply", integer_to_list(NodeHash)), PidParts0),
    RecomposedEncoded = base64:encode(pid_recomposition:to_binary(PidParts)),

    Suffix = <<RecomposedEncoded/binary, ".", Key/binary>>,
    {Key, Suffix}.

-spec decode_reply_to(binary(), #{non_neg_integer() => node()}) ->
    {ok, pid(), binary()} | {error, any()}.
decode_reply_to(Bin, CandidateNodes) ->
    try
        [PidEnc, Key] = binary:split(Bin, <<".">>),
        RawPidBin = base64:decode(PidEnc),
        PidParts0 = #{node := ShortenedNodename} = pid_recomposition:from_binary(RawPidBin),
        {_, NodeHash} = rabbit_nodes_common:parts(ShortenedNodename),
        case maps:get(list_to_integer(NodeHash), CandidateNodes, undefined) of
            undefined ->
                {error, target_node_not_found};
            Candidate ->
                PidParts = maps:update(node, Candidate, PidParts0),
                {ok, pid_recomposition:recompose(PidParts), Key}
        end
    catch
        error:_ ->
            {error, unrecognized_format}
    end.
