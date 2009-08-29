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

-export([append/4, read/2, scan/1]).

%%----------------------------------------------------------------------------

-define(INTEGER_SIZE_BYTES,      8).
-define(INTEGER_SIZE_BITS,       (8 * ?INTEGER_SIZE_BYTES)).
-define(WRITE_OK_SIZE_BITS,      8).
-define(WRITE_OK_TRANSIENT,      255).
-define(WRITE_OK_PERSISTENT,     254).
-define(FILE_PACKING_ADJUSTMENT, (1 + (2* (?INTEGER_SIZE_BYTES)))).

%%----------------------------------------------------------------------------

append(FileHdl, MsgId, MsgBody, IsPersistent) when is_binary(MsgBody) ->
    BodySize = size(MsgBody),
    MsgIdBin = term_to_binary(MsgId),
    MsgIdBinSize = size(MsgIdBin),
    Size = BodySize + MsgIdBinSize,
    StopByte = case IsPersistent of
                   true -> ?WRITE_OK_PERSISTENT;
                   false -> ?WRITE_OK_TRANSIENT
               end,
    case file:write(FileHdl, <<Size:?INTEGER_SIZE_BITS,
                               MsgIdBinSize:?INTEGER_SIZE_BITS,
                               MsgIdBin:MsgIdBinSize/binary,
                               MsgBody:BodySize/binary,
                               StopByte:?WRITE_OK_SIZE_BITS>>) of
        ok -> {ok, Size + ?FILE_PACKING_ADJUSTMENT};
        KO -> KO
    end.

read(FileHdl, TotalSize) ->
    Size = TotalSize - ?FILE_PACKING_ADJUSTMENT,
    SizeWriteOkBytes = Size + 1,
    case file:read(FileHdl, TotalSize) of
        {ok, <<Size:?INTEGER_SIZE_BITS,
               MsgIdBinSize:?INTEGER_SIZE_BITS,
               Rest:SizeWriteOkBytes/binary>>} ->
            BodySize = Size - MsgIdBinSize,
            <<_MsgId:MsgIdBinSize/binary, MsgBody:BodySize/binary,
             StopByte:?WRITE_OK_SIZE_BITS>> = Rest,
            Persistent = case StopByte of
                             ?WRITE_OK_TRANSIENT  -> false;
                             ?WRITE_OK_PERSISTENT -> true
                         end,
            {ok, {MsgBody, Persistent, BodySize}};
        KO -> KO
    end.

scan(FileHdl) -> scan(FileHdl, 0, []).

scan(FileHdl, Offset, Acc) ->
    case read_next(FileHdl, Offset) of
        eof -> {ok, Acc};
        {corrupted, NextOffset} ->
            scan(FileHdl, NextOffset, Acc);
        {ok, {MsgId, IsPersistent, TotalSize, NextOffset}} ->
            scan(FileHdl, NextOffset,
                 [{MsgId, IsPersistent, TotalSize, Offset} | Acc]);
        _KO ->
            %% bad message, but we may still have recovered some valid messages
            {ok, Acc}
    end.

read_next(FileHdl, Offset) ->
    TwoIntegers = 2 * ?INTEGER_SIZE_BYTES,
    case file:read(FileHdl, TwoIntegers) of
        {ok,
         <<Size:?INTEGER_SIZE_BITS, MsgIdBinSize:?INTEGER_SIZE_BITS>>} ->
            case {Size, MsgIdBinSize} of
                {0, _} -> eof; %% Nothing we can do other than stop
                {_, 0} ->
                    %% current message corrupted, try skipping past it
                    ExpectedAbsPos = Offset + Size + ?FILE_PACKING_ADJUSTMENT,
                    case file:position(FileHdl, {cur, Size + 1}) of
                        {ok, ExpectedAbsPos} -> {corrupted, ExpectedAbsPos};
                        {ok, _SomeOtherPos}  -> eof; %% seek failed, so give up
                        KO                   -> KO
                    end;
                {_, _} -> %% all good, let's continue
                    case file:read(FileHdl, MsgIdBinSize) of
                        {ok, <<MsgIdBin:MsgIdBinSize/binary>>} ->
                            TotalSize = Size + ?FILE_PACKING_ADJUSTMENT,
                            ExpectedAbsPos = Offset + TotalSize - 1,
                            case file:position(
                                   FileHdl, {cur, Size - MsgIdBinSize}) of
                                {ok, ExpectedAbsPos} ->
                                    NextOffset = ExpectedAbsPos + 1,
                                    case read_stop_byte(FileHdl) of
                                        {ok, Persistent} ->
                                            MsgId = binary_to_term(MsgIdBin),
                                            {ok, {MsgId, Persistent,
                                                  TotalSize, NextOffset}};
                                        corrupted ->
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

read_stop_byte(FileHdl) ->
    case file:read(FileHdl, 1) of
        {ok, <<?WRITE_OK_TRANSIENT:?WRITE_OK_SIZE_BITS>>}  -> {ok, false};
        {ok, <<?WRITE_OK_PERSISTENT:?WRITE_OK_SIZE_BITS>>} -> {ok, true};
        {ok, _SomeOtherData}                               -> corrupted;
        KO                                                 -> KO
    end.
