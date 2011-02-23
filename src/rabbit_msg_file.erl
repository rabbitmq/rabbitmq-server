%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at http://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and
%% limitations under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is VMware, Inc.
%% Copyright (c) 2007-2011 VMware, Inc.  All rights reserved.
%%

-module(rabbit_msg_file).

-export([append/3, read/2, scan/2]).

%%----------------------------------------------------------------------------

-include("rabbit_msg_store.hrl").

-define(INTEGER_SIZE_BYTES,      8).
-define(INTEGER_SIZE_BITS,       (8 * ?INTEGER_SIZE_BYTES)).
-define(WRITE_OK_SIZE_BITS,      8).
-define(WRITE_OK_MARKER,         255).
-define(FILE_PACKING_ADJUSTMENT, (1 + ?INTEGER_SIZE_BYTES)).
-define(GUID_SIZE_BYTES,         16).
-define(GUID_SIZE_BITS,          (8 * ?GUID_SIZE_BYTES)).
-define(SCAN_BLOCK_SIZE,         4194304). %% 4MB

%%----------------------------------------------------------------------------

-ifdef(use_specs).

-type(io_device() :: any()).
-type(position() :: non_neg_integer()).
-type(msg_size() :: non_neg_integer()).
-type(file_size() :: non_neg_integer()).

-spec(append/3 :: (io_device(), rabbit_guid:guid(), msg()) ->
                       rabbit_types:ok_or_error2(msg_size(), any())).
-spec(read/2 :: (io_device(), msg_size()) ->
                     rabbit_types:ok_or_error2({rabbit_guid:guid(), msg()},
                                               any())).
-spec(scan/2 :: (io_device(), file_size()) ->
                     {'ok', [{rabbit_guid:guid(), msg_size(), position()}],
                      position()}).

-endif.

%%----------------------------------------------------------------------------

append(FileHdl, Guid, MsgBody)
  when is_binary(Guid) andalso size(Guid) =:= ?GUID_SIZE_BYTES ->
    MsgBodyBin  = term_to_binary(MsgBody),
    MsgBodyBinSize = size(MsgBodyBin),
    Size = MsgBodyBinSize + ?GUID_SIZE_BYTES,
    case file_handle_cache:append(FileHdl,
                                  <<Size:?INTEGER_SIZE_BITS,
                                   Guid:?GUID_SIZE_BYTES/binary,
                                   MsgBodyBin:MsgBodyBinSize/binary,
                                   ?WRITE_OK_MARKER:?WRITE_OK_SIZE_BITS>>) of
        ok -> {ok, Size + ?FILE_PACKING_ADJUSTMENT};
        KO -> KO
    end.

read(FileHdl, TotalSize) ->
    Size = TotalSize - ?FILE_PACKING_ADJUSTMENT,
    BodyBinSize = Size - ?GUID_SIZE_BYTES,
    case file_handle_cache:read(FileHdl, TotalSize) of
        {ok, <<Size:?INTEGER_SIZE_BITS,
              Guid:?GUID_SIZE_BYTES/binary,
              MsgBodyBin:BodyBinSize/binary,
              ?WRITE_OK_MARKER:?WRITE_OK_SIZE_BITS>>} ->
            {ok, {Guid, binary_to_term(MsgBodyBin)}};
        KO -> KO
    end.

scan(FileHdl, FileSize) when FileSize >= 0 ->
    scan(FileHdl, FileSize, <<>>, 0, [], 0).

scan(_FileHdl, FileSize, _Data, FileSize, Acc, ScanOffset) ->
    {ok, Acc, ScanOffset};
scan(FileHdl, FileSize, Data, ReadOffset, Acc, ScanOffset) ->
    Read = lists:min([?SCAN_BLOCK_SIZE, (FileSize - ReadOffset)]),
    case file_handle_cache:read(FileHdl, Read) of
        {ok, Data1} ->
            {Data2, Acc1, ScanOffset1} =
                scan(<<Data/binary, Data1/binary>>, Acc, ScanOffset),
            ReadOffset1 = ReadOffset + size(Data1),
            scan(FileHdl, FileSize, Data2, ReadOffset1, Acc1, ScanOffset1);
        _KO ->
            {ok, Acc, ScanOffset}
    end.

scan(<<>>, Acc, Offset) ->
    {<<>>, Acc, Offset};
scan(<<0:?INTEGER_SIZE_BITS, _Rest/binary>>, Acc, Offset) ->
    {<<>>, Acc, Offset}; %% Nothing to do other than stop.
scan(<<Size:?INTEGER_SIZE_BITS, GuidAndMsg:Size/binary,
       WriteMarker:?WRITE_OK_SIZE_BITS, Rest/binary>>, Acc, Offset) ->
    TotalSize = Size + ?FILE_PACKING_ADJUSTMENT,
    case WriteMarker of
        ?WRITE_OK_MARKER ->
            %% Here we take option 5 from
            %% http://www.erlang.org/cgi-bin/ezmlm-cgi?2:mss:1569 in
            %% which we read the Guid as a number, and then convert it
            %% back to a binary in order to work around bugs in
            %% Erlang's GC.
            <<GuidNum:?GUID_SIZE_BITS, _Msg/binary>> =
                <<GuidAndMsg:Size/binary>>,
            <<Guid:?GUID_SIZE_BYTES/binary>> = <<GuidNum:?GUID_SIZE_BITS>>,
            scan(Rest, [{Guid, TotalSize, Offset} | Acc], Offset + TotalSize);
        _ ->
            scan(Rest, Acc, Offset + TotalSize)
    end;
scan(Data, Acc, Offset) ->
    {Data, Acc, Offset}.
