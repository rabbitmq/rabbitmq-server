%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2022 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_msg_file).

-export([append/3, read/2, scan/4]).

%%----------------------------------------------------------------------------

-include_lib("rabbit_common/include/rabbit_msg_store.hrl").

-define(INTEGER_SIZE_BYTES,      8).
-define(INTEGER_SIZE_BITS,       (8 * ?INTEGER_SIZE_BYTES)).
-define(WRITE_OK_SIZE_BITS,      8).
-define(WRITE_OK_MARKER,         255).
-define(FILE_PACKING_ADJUSTMENT, (1 + ?INTEGER_SIZE_BYTES)).
-define(MSG_ID_SIZE_BYTES,       16).
-define(MSG_ID_SIZE_BITS,        (8 * ?MSG_ID_SIZE_BYTES)).
-define(SCAN_BLOCK_SIZE,         4194304). %% 4MB

%%----------------------------------------------------------------------------

-type io_device() :: any().
-type position() :: non_neg_integer().
-type msg_size() :: non_neg_integer().
-type file_size() :: non_neg_integer().
-type message_accumulator(A) ::
        fun (({rabbit_types:msg_id(), msg_size(), position(), binary()}, A) ->
            A).

%%----------------------------------------------------------------------------

-spec append(io_device(), rabbit_types:msg_id(), msg()) ->
          rabbit_types:ok_or_error2(msg_size(), any()).

append(FileHdl, MsgId, MsgBody)
  when is_binary(MsgId) andalso size(MsgId) =:= ?MSG_ID_SIZE_BYTES ->
    MsgBodyBin  = term_to_binary(MsgBody),
    MsgBodyBinSize = size(MsgBodyBin),
    Size = MsgBodyBinSize + ?MSG_ID_SIZE_BYTES,
    case file_handle_cache:append(FileHdl,
                                  <<Size:?INTEGER_SIZE_BITS,
                                    MsgId:?MSG_ID_SIZE_BYTES/binary,
                                    MsgBodyBin:MsgBodyBinSize/binary,
                                    ?WRITE_OK_MARKER:?WRITE_OK_SIZE_BITS>>) of
        ok -> {ok, Size + ?FILE_PACKING_ADJUSTMENT};
        KO -> KO
    end.

-spec read(io_device(), msg_size()) ->
          rabbit_types:ok_or_error2({rabbit_types:msg_id(), msg()},
                                    any()).

read(FileHdl, TotalSize) ->
    Size = TotalSize - ?FILE_PACKING_ADJUSTMENT,
    BodyBinSize = Size - ?MSG_ID_SIZE_BYTES,
    case file_handle_cache:read(FileHdl, TotalSize) of
        {ok, <<Size:?INTEGER_SIZE_BITS,
               MsgId:?MSG_ID_SIZE_BYTES/binary,
               MsgBodyBin:BodyBinSize/binary,
               ?WRITE_OK_MARKER:?WRITE_OK_SIZE_BITS>>} ->
            {ok, {MsgId, binary_to_term(MsgBodyBin)}};
        KO -> KO
    end.

-spec scan(io_device(), file_size(), message_accumulator(A), A) ->
          {'ok', A, position()}.

scan(FileHdl, FileSize, Fun, Acc) when FileSize >= 0 ->
    scan(FileHdl, FileSize, <<>>, 0, 0, Fun, Acc).

scan(_FileHdl, FileSize, _Data, FileSize, ScanOffset, _Fun, Acc) ->
    {ok, Acc, ScanOffset};
scan(FileHdl, FileSize, Data, ReadOffset, ScanOffset, Fun, Acc) ->
    Read = lists:min([?SCAN_BLOCK_SIZE, (FileSize - ReadOffset)]),
    case file_handle_cache:read(FileHdl, Read) of
        {ok, Data1} ->
            {Data2, Acc1, ScanOffset1} =
                scanner(<<Data/binary, Data1/binary>>, ScanOffset, Fun, Acc),
            ReadOffset1 = ReadOffset + size(Data1),
            scan(FileHdl, FileSize, Data2, ReadOffset1, ScanOffset1, Fun, Acc1);
        _KO ->
            {ok, Acc, ScanOffset}
    end.

scanner(<<>>, Offset, _Fun, Acc) ->
    {<<>>, Acc, Offset};
scanner(<<0:?INTEGER_SIZE_BITS, _Rest/binary>>, Offset, _Fun, Acc) ->
    {<<>>, Acc, Offset}; %% Nothing to do other than stop.
scanner(<<Size:?INTEGER_SIZE_BITS, MsgIdAndMsg:Size/binary,
          WriteMarker:?WRITE_OK_SIZE_BITS, Rest/binary>>, Offset, Fun, Acc) ->
    TotalSize = Size + ?FILE_PACKING_ADJUSTMENT,
    case WriteMarker of
        ?WRITE_OK_MARKER ->
            %% Here we take option 5 from
            %% https://www.erlang.org/cgi-bin/ezmlm-cgi?2:mss:1569 in
            %% which we read the MsgId as a number, and then convert it
            %% back to a binary in order to work around bugs in
            %% Erlang's GC.
            <<MsgIdNum:?MSG_ID_SIZE_BITS, Msg/binary>> =
                <<MsgIdAndMsg:Size/binary>>,
            <<MsgId:?MSG_ID_SIZE_BYTES/binary>> =
                <<MsgIdNum:?MSG_ID_SIZE_BITS>>,
            scanner(Rest, Offset + TotalSize, Fun,
                    Fun({MsgId, TotalSize, Offset, Msg}, Acc));
        _ ->
            scanner(Rest, Offset + TotalSize, Fun, Acc)
    end;
scanner(Data, Offset, _Fun, Acc) ->
    {Data, Acc, Offset}.
