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

-module(rabbit_disk_queue).

-compile(export_all). %% CHANGE ME

-define(WRITE_OK_SIZE_BITS, 8).
-define(WRITE_OK, 255).
-define(INTEGER_SIZE_BYTES, 8).
-define(INTEGER_SIZE_BITS, 8 * ?INTEGER_SIZE_BYTES).

base_filename() ->
    mnesia:system_info(directory) ++ "/rabbit_disk_queue/".

append_message(FileHdl, MsgId, MsgBody) when is_binary(MsgBody) ->
    BodySize = size(MsgBody),
    MsgIdBin = term_to_binary(MsgId),
    MsgIdBinSize = size(MsgIdBin),
    TotalSize = BodySize + MsgIdBinSize,
    case file:write(FileHdl, <<TotalSize:(?INTEGER_SIZE_BITS),
			       MsgIdBinSize:(?INTEGER_SIZE_BITS),
			       MsgIdBin:MsgIdBinSize/binary, MsgBody:BodySize/binary>>) of
	ok -> file:write(FileHdl, <<(?WRITE_OK):(?WRITE_OK_SIZE_BITS)>>);
	KO -> KO
    end.

read_message_at_offset(FileHdl, Offset) ->
    case file:position(FileHdl, {bof, Offset}) of
	{ok, Offset} ->
	    case file:read(FileHdl, 2 * (?INTEGER_SIZE_BYTES)) of
		{ok, <<TotalSize:(?INTEGER_SIZE_BITS), MsgIdBinSize:(?INTEGER_SIZE_BITS)>>} ->
		    ExpectedAbsPos = Offset + (2 * (?INTEGER_SIZE_BYTES)) + MsgIdBinSize,
		    case file:position(FileHdl, {cur, MsgIdBinSize}) of
			{ok, ExpectedAbsPos} ->
			    BodySize = TotalSize - MsgIdBinSize,
			    case file:read(FileHdl, 1 + BodySize) of
				{ok, <<MsgBody:BodySize/binary, 255:(?WRITE_OK_SIZE_BITS)>>} ->
				    {ok, MsgBody, BodySize};
				KO -> KO
			    end;
			KO -> KO
		    end;
		KO -> KO
	    end;
	KO -> KO
    end.
