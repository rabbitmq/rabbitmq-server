%%   The contents of this file are subject to the Mozilla Public License
%%   Version 1.1 (the "License"); you may not use this file except in
%%   compliance with the License. You may obtain a copy of the License at
%%   http://www.mozilla.org/MPL/
%%
%%   Software distributed under the License is distributed on an "AS IS"
%%   basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See the
%%   License for the specific language governing rights and limitations
%%   under the License.
%%
%%   The Original Code is RabbitMQ.
%%
%%   The Initial Developers of the Original Code are LShift Ltd,
%%   Cohesive Financial Technologies LLC, and Rabbit Technologies Ltd.
%%
%%   Portions created before 22-Nov-2008 00:00:00 GMT by LShift Ltd,
%%   Cohesive Financial Technologies LLC, or Rabbit Technologies Ltd
%%   are Copyright (C) 2007-2008 LShift Ltd, Cohesive Financial
%%   Technologies LLC, and Rabbit Technologies Ltd.
%%
%%   Portions created by LShift Ltd are Copyright (C) 2007-2009 LShift
%%   Ltd. Portions created by Cohesive Financial Technologies LLC are
%%   Copyright (C) 2007-2009 Cohesive Financial Technologies
%%   LLC. Portions created by Rabbit Technologies Ltd are Copyright
%%   (C) 2007-2009 Rabbit Technologies Ltd.
%%
%%   All Rights Reserved.
%%
%%   Contributor(s): ______________________________________.
%%

-module(rabbit_msg_file).

-export([append/3, read/2, scan/1]).

%%----------------------------------------------------------------------------

-define(INTEGER_SIZE_BYTES,      8).
-define(INTEGER_SIZE_BITS,       (8 * ?INTEGER_SIZE_BYTES)).
-define(WRITE_OK_SIZE_BITS,      8).
-define(WRITE_OK_MARKER,         255).
-define(FILE_PACKING_ADJUSTMENT, (1 + (2 * (?INTEGER_SIZE_BYTES)))).

%%----------------------------------------------------------------------------

-ifdef(use_specs).

-type(io_device() :: any()).
-type(msg_id() :: binary()).
-type(msg() :: any()).
-type(position() :: non_neg_integer()).
-type(msg_size() :: non_neg_integer()).

-spec(append/3 :: (io_device(), msg_id(), msg()) ->
             ({'ok', msg_size()} | {'error', any()})).
-spec(read/2 :: (io_device(), msg_size()) ->
             ({'ok', {msg_id(), msg()}} | {'error', any()})).
-spec(scan/1 :: (io_device()) ->
             {'ok', [{msg_id(), msg_size(), position()}]}).

-endif.

%%----------------------------------------------------------------------------

append(FileHdl, MsgId, MsgBody) when is_binary(MsgId) ->
    MsgBodyBin  = term_to_binary(MsgBody),
    [MsgIdSize, MsgBodyBinSize] = Sizes = [size(B) || B <- [MsgId, MsgBodyBin]],
    Size = lists:sum(Sizes),
    case file:write(FileHdl, <<Size:?INTEGER_SIZE_BITS,
                               MsgIdSize:?INTEGER_SIZE_BITS,
                               MsgId:MsgIdSize/binary,
                               MsgBodyBin:MsgBodyBinSize/binary,
                               ?WRITE_OK_MARKER:?WRITE_OK_SIZE_BITS>>) of
        ok -> {ok, Size + ?FILE_PACKING_ADJUSTMENT};
        KO -> KO
    end.

read(FileHdl, TotalSize) ->
    Size = TotalSize - ?FILE_PACKING_ADJUSTMENT,
    SizeWriteOkBytes = Size + 1,
    case file:read(FileHdl, TotalSize) of
        {ok, <<Size:?INTEGER_SIZE_BITS,
               MsgIdSize:?INTEGER_SIZE_BITS,
               Rest:SizeWriteOkBytes/binary>>} ->
            BodyBinSize = Size - MsgIdSize,
            <<MsgId:MsgIdSize/binary,
              MsgBodyBin:BodyBinSize/binary,
              ?WRITE_OK_MARKER:?WRITE_OK_SIZE_BITS>> = Rest,
            {ok, {MsgId, binary_to_term(MsgBodyBin)}};
        KO -> KO
    end.

scan(FileHdl) -> scan(FileHdl, 0, []).

scan(FileHdl, Offset, Acc) ->
    case read_next(FileHdl, Offset) of
        eof -> {ok, Acc};
        {corrupted, NextOffset} ->
            scan(FileHdl, NextOffset, Acc);
        {ok, {MsgId, TotalSize, NextOffset}} ->
            scan(FileHdl, NextOffset, [{MsgId, TotalSize, Offset} | Acc]);
        _KO ->
            %% bad message, but we may still have recovered some valid messages
            {ok, Acc}
    end.

read_next(FileHdl, Offset) ->
    TwoIntegers = 2 * ?INTEGER_SIZE_BYTES,
    case file:read(FileHdl, TwoIntegers) of
        {ok, <<Size:?INTEGER_SIZE_BITS, MsgIdSize:?INTEGER_SIZE_BITS>>} ->
            if Size == 0 -> eof; %% Nothing we can do other than stop
               MsgIdSize == 0 ->
                    %% current message corrupted, try skipping past it
                    ExpectedAbsPos = Offset + Size + ?FILE_PACKING_ADJUSTMENT,
                    case file:position(FileHdl, {cur, Size + 1}) of
                        {ok, ExpectedAbsPos} -> {corrupted, ExpectedAbsPos};
                        {ok, _SomeOtherPos}  -> eof; %% seek failed, so give up
                        KO                   -> KO
                    end;
               true -> %% all good, let's continue
                    case file:read(FileHdl, MsgIdSize) of
                        {ok, <<MsgId:MsgIdSize/binary>>} ->
                            TotalSize = Size + ?FILE_PACKING_ADJUSTMENT,
                            ExpectedAbsPos = Offset + TotalSize - 1,
                            case file:position(
                                   FileHdl, {cur, Size - MsgIdSize}) of
                                {ok, ExpectedAbsPos} ->
                                    NextOffset = ExpectedAbsPos + 1,
                                    case file:read(FileHdl, 1) of
                                        {ok, <<?WRITE_OK_MARKER:
                                               ?WRITE_OK_SIZE_BITS>>} ->
                                            {ok, {MsgId,
                                                  TotalSize, NextOffset}};
                                        {ok, _SomeOtherData} ->
                                            {corrupted, NextOffset};
                                        KO -> KO
                                    end;
                                {ok, _SomeOtherPos} ->
                                    %% seek failed, so give up
                                    eof;
                                KO -> KO
                            end;
                        Other -> Other
                    end
            end;
        Other -> Other
    end.
