%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_direct_reply_to).

%% API
-export([
    %% Original amq.rabbitmq.reply-to target channel encoding
    compute_key_and_suffix_v1/1,
    decode_reply_to_v1/1,

    %% v2 amq.rabbitmq.reply-to target channel encoding
    compute_key_and_suffix_v2/1,
    decode_reply_to_v2/2
]).

%%
%% API
%%

-type decoded_pid_and_key() :: {ok, pid(), binary()} | {error, any()}.

-spec compute_key_and_suffix_v1(pid()) -> {binary(), binary()}.
%% This original pid encoding function produces values that exceed routing key length limit
%% on nodes with long (say, 130+ characters) node names.
compute_key_and_suffix_v1(Pid) ->
    Key = base64:encode(rabbit_guid:gen()),
    PidEnc = base64:encode(term_to_binary(Pid)),
    Suffix = <<PidEnc/binary, ".", Key/binary>>,
    {Key, Suffix}.

-spec decode_reply_to_v1(binary()) -> decoded_pid_and_key() | {error, any()}.
decode_reply_to_v1(Bin) ->
    case string:lexemes(Bin, ".") of
        [PidEnc, Key] -> Pid = binary_to_term(base64:decode(PidEnc)),
                         {ok, Pid, unicode:characters_to_binary(Key)};
        _             -> {error, unrecognized_format}
    end.


-spec compute_key_and_suffix_v2(pid()) -> {binary(), binary()}.
%% This pid encoding function produces values that are of mostly fixed size
%% regardless of the node name length.
compute_key_and_suffix_v2(Pid) ->
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

-spec decode_reply_to_v2(binary(), #{non_neg_integer() => node()}) -> decoded_pid_and_key() | {error, any()}.
decode_reply_to_v2(Bin, CandidateNodes) ->
    case string:lexemes(Bin, ".") of
        [PidEnc, Key] ->
            RawPidBin = base64:decode(PidEnc),
            PidParts0 = #{node := ShortenedNodename} = pid_recomposition:from_binary(RawPidBin),
            {_, NodeHash} = rabbit_nodes_common:parts(ShortenedNodename),
            case maps:get(list_to_integer(NodeHash), CandidateNodes, undefined) of
                undefined -> error;
                Candidate ->
                    PidParts = maps:update(node, Candidate, PidParts0),
                    {ok, pid_recomposition:recompose(PidParts), unicode:characters_to_binary(Key)}
            end;
        _             -> {error, unrecognized_format}
    end.
