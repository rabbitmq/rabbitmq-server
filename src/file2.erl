%% This is a version of 'file' from R14B03, which runs calls to
%% file_server2 thorugh the worker_pool and uses the file_name/1
%% function from R12B-5. Use this module when you expect a large
%% number of concurrent file operations.

%%
%% %CopyrightBegin%
%%
%% Copyright Ericsson AB 1996-2011. All Rights Reserved.
%%
%% The contents of this file are subject to the Erlang Public License,
%% Version 1.1, (the "License"); you may not use this file except in
%% compliance with the License. You should have received a copy of the
%% Erlang Public License along with this software. If not, it can be
%% retrieved online at http://www.erlang.org/.
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and limitations
%% under the License.
%%
%% %CopyrightEnd%
%%
-module(file2).

%% Interface module for the file server and the file io servers.



%%% External exports

-export([format_error/1]).
%% File system and metadata.
-export([get_cwd/0, get_cwd/1, set_cwd/1, delete/1, rename/2,
	 make_dir/1, del_dir/1, list_dir/1,
	 read_file_info/1, write_file_info/2,
	 altname/1,
	 read_link_info/1, read_link/1,
	 make_link/2, make_symlink/2,
	 read_file/1, write_file/2, write_file/3]).
%% Specialized
-export([ipread_s32bu_p32bu/3]).
%% Generic file contents.
-export([open/2, close/1, advise/4,
	 read/2, write/2, 
	 pread/2, pread/3, pwrite/2, pwrite/3,
	 read_line/1,
	 position/2, truncate/1, datasync/1, sync/1,
	 copy/2, copy/3]).
%% High level operations
-export([consult/1, path_consult/2]).
-export([eval/1, eval/2, path_eval/2, path_eval/3, path_open/3]).
-export([script/1, script/2, path_script/2, path_script/3]).
-export([change_owner/2, change_owner/3, change_group/2,
	 change_mode/2, change_time/2, change_time/3]).

-export([pid2name/1]).

%%% Obsolete exported functions

-export([raw_read_file_info/1, raw_write_file_info/2]).

%% Internal export to prim_file and ram_file until they implement
%% an efficient copy themselves.
-export([copy_opened/3]).

-export([ipread_s32bu_p32bu_int/3]).

%%% Includes and defines
-include_lib("kernel/include/file.hrl").

-define(FILE_IO_SERVER_TABLE, file_io_servers).

-define(FILE_SERVER, file_server_2).   % Registered name
-define(PRIM_FILE, prim_file).         % Module
-define(RAM_FILE, ram_file).           % Module

%% data types
-type deep_list() :: [char() | atom() | deep_list()].

%%%-----------------------------------------------------------------
%%% General functions

format_error({_Line, ?MODULE, undefined_script}) ->
    "no value returned from script";
format_error({Line, ?MODULE, {Class, Reason, Stacktrace}}) ->
    io_lib:format("~w: evaluation failed with reason ~w:~w and stacktrace ~w", 
                  [Line, Class, Reason, Stacktrace]);
format_error({Line, ?MODULE, {Reason, Stacktrace}}) ->
    io_lib:format("~w: evaluation failed with reason ~w and stacktrace ~w", 
                  [Line, Reason, Stacktrace]);
format_error({Line, Mod, Reason}) ->
    io_lib:format("~w: ~s", [Line, Mod:format_error(Reason)]);
format_error(badarg) ->
    "bad argument";
format_error(system_limit) ->
    "a system limit was hit, probably not enough ports";
format_error(terminated) ->
    "the file server process is terminated";
format_error(ErrorId) ->
    erl_posix_msg:message(ErrorId).

pid2name(Pid) when is_pid(Pid) ->
    case whereis(?FILE_SERVER) of
	undefined ->
	    undefined;
	_ ->
	    case ets:lookup(?FILE_IO_SERVER_TABLE, Pid) of
		[{_, Name} | _] ->
		    {ok, Name};
		_ ->
		    undefined
	    end
    end.

%%%-----------------------------------------------------------------
%%% File server functions.
%%% Functions that do not operate on a single open file.
%%% Stateless.
get_cwd() ->
    call(get_cwd, []).

get_cwd(Drive) ->
    check_and_call(get_cwd, [file_name(Drive)]).

set_cwd(Dirname) -> 
    check_and_call(set_cwd, [file_name(Dirname)]).

delete(Name) ->
    check_and_call(delete, [file_name(Name)]).

rename(From, To) ->
    check_and_call(rename, [file_name(From), file_name(To)]).

make_dir(Name) ->
    check_and_call(make_dir, [file_name(Name)]).

del_dir(Name) ->
    check_and_call(del_dir, [file_name(Name)]).

read_file_info(Name) ->
    check_and_call(read_file_info, [file_name(Name)]).

altname(Name) ->
    check_and_call(altname, [file_name(Name)]).

read_link_info(Name) ->
    check_and_call(read_link_info, [file_name(Name)]).

read_link(Name) ->
    check_and_call(read_link, [file_name(Name)]).

write_file_info(Name, Info = #file_info{}) ->
    check_and_call(write_file_info, [file_name(Name), Info]).

list_dir(Name) ->
    check_and_call(list_dir, [file_name(Name)]).

read_file(Name) ->
    check_and_call(read_file, [file_name(Name)]).

make_link(Old, New) ->
    check_and_call(make_link, [file_name(Old), file_name(New)]).

make_symlink(Old, New) ->
    check_and_call(make_symlink, [file_name(Old), file_name(New)]).

write_file(Name, Bin) ->
    check_and_call(write_file, [file_name(Name), make_binary(Bin)]).

%% This whole operation should be moved to the file_server and prim_file
%% when it is time to change file server protocol again.
%% Meanwhile, it is implemented here, slightly less efficient.

write_file(Name, Bin, ModeList) when is_list(ModeList) ->
    case make_binary(Bin) of
	B when is_binary(B) ->
	    case open(Name, [binary, write | 
			     lists:delete(binary, 
					  lists:delete(write, ModeList))]) of
		{ok, Handle} ->
		    case write(Handle, B) of
			ok ->
			    close(Handle);
			E1 ->
			    close(Handle),
			    E1
		    end;
		E2 ->
		    E2
	    end;
	E3 ->
	    E3
    end.

%% Obsolete, undocumented, local node only, don't use!.
%% XXX to be removed.
raw_read_file_info(Name) ->
    Args = [file_name(Name)],
    case check_args(Args) of
	ok ->
	    [FileName] = Args,
	    ?PRIM_FILE:read_file_info(FileName);
	Error ->
	    Error
    end.

%% Obsolete, undocumented, local node only, don't use!.
%% XXX to be removed.
raw_write_file_info(Name, #file_info{} = Info) ->
    Args = [file_name(Name)],
    case check_args(Args) of
	ok ->
	    [FileName] = Args,
	    ?PRIM_FILE:write_file_info(FileName, Info);
	Error ->
	    Error
    end.

%%%-----------------------------------------------------------------
%%% File io server functions.
%%% They operate on a single open file.
%%% Stateful.

%% Contemporary mode specification - list of options

open(Item, ModeList) when is_list(ModeList) ->
    case lists:member(raw, ModeList) of
	%% Raw file, use ?PRIM_FILE to handle this file
	true ->
	    %% check if raw file mode is disabled
	    case catch application:get_env(kernel, raw_files) of
		{ok,false} ->
		    open(Item, lists:delete(raw, ModeList));
		_ ->				% undefined | {ok,true}
		    Args = [file_name(Item) | ModeList],
		    case check_args(Args) of
			ok ->
			    [FileName | _] = Args,
			    %% We rely on the returned Handle (in {ok, Handle})
			    %% being a pid() or a #file_descriptor{}
			    ?PRIM_FILE:open(FileName, ModeList);
			Error ->
			    Error
		    end
	    end;
	false ->
	    case lists:member(ram, ModeList) of
		%% RAM file, use ?RAM_FILE to handle this file
		true ->
		    case check_args(ModeList) of
			ok ->
			    ?RAM_FILE:open(Item, ModeList);
			Error ->
			    Error
		    end;
		%% File server file
		false ->
		    Args = [file_name(Item) | ModeList],
		    case check_args(Args) of 
			ok ->
			    [FileName | _] = Args,
			    call(open, [FileName, ModeList]);
			Error ->
			    Error
		    end
	    end
    end;
%% Old obsolete mode specification in atom or 2-tuple format
open(Item, Mode) ->
    open(Item, mode_list(Mode)).

%%%-----------------------------------------------------------------
%%% The following interface functions operate on open files.
%%% The File argument must be either a Pid or a handle 
%%% returned from ?PRIM_FILE:open.

close(File) when is_pid(File) ->
    R = file_request(File, close),
    case wait_file_reply(File, R) of
	{error, terminated} ->
	    ok;
	Other ->
	    Other
    end;
%%    unlink(File),
%%    exit(File, close),
%%    ok;
close(#file_descriptor{module = Module} = Handle) ->
    Module:close(Handle);
close(_) ->
    {error, badarg}.

advise(File, Offset, Length, Advise) when is_pid(File) ->
    R = file_request(File, {advise, Offset, Length, Advise}),
    wait_file_reply(File, R);
advise(#file_descriptor{module = Module} = Handle, Offset, Length, Advise) ->
    Module:advise(Handle, Offset, Length, Advise);
advise(_, _, _, _) ->
    {error, badarg}.

read(File, Sz) when (is_pid(File) orelse is_atom(File)), is_integer(Sz), Sz >= 0 ->
    case io:request(File, {get_chars, '', Sz}) of
	Data when is_list(Data); is_binary(Data) ->
	    {ok, Data};
	Other ->
	    Other
    end;
read(#file_descriptor{module = Module} = Handle, Sz) 
  when is_integer(Sz), Sz >= 0 ->
    Module:read(Handle, Sz);
read(_, _) ->
    {error, badarg}.

read_line(File) when (is_pid(File) orelse is_atom(File)) ->
    case io:request(File, {get_line, ''}) of
	Data when is_list(Data); is_binary(Data) ->
	    {ok, Data};
	Other ->
	    Other
    end;
read_line(#file_descriptor{module = Module} = Handle) ->
    Module:read_line(Handle);
read_line(_) ->
    {error, badarg}.

pread(File, L) when is_pid(File), is_list(L) ->
    pread_int(File, L, []);
pread(#file_descriptor{module = Module} = Handle, L) when is_list(L) ->
    Module:pread(Handle, L);
pread(_, _) ->
    {error, badarg}.

pread_int(_File, [], R) ->
    {ok, lists:reverse(R)};
pread_int(File, [{At, Sz} | T], R) when is_integer(Sz), Sz >= 0 ->
    case pread(File, At, Sz) of
	{ok, Data} ->
	    pread_int(File, T, [Data | R]);
	eof ->
	    pread_int(File, T, [eof | R]);
	{error, _} = Error ->
	    Error
    end;
pread_int(_, _, _) ->
    {error, badarg}.

pread(File, At, Sz) when is_pid(File), is_integer(Sz), Sz >= 0 ->
    R = file_request(File, {pread, At, Sz}),
    wait_file_reply(File, R);
pread(#file_descriptor{module = Module} = Handle, Offs, Sz) 
  when is_integer(Sz), Sz >= 0 ->
    Module:pread(Handle, Offs, Sz);
pread(_, _, _) ->
    {error, badarg}.

write(File, Bytes) when (is_pid(File) orelse is_atom(File)) ->
    case make_binary(Bytes) of
	Bin when is_binary(Bin) ->
	    io:request(File, {put_chars,Bin});
	Error ->
	    Error
    end;
write(#file_descriptor{module = Module} = Handle, Bytes) ->
    Module:write(Handle, Bytes);
write(_, _) ->
    {error, badarg}.

pwrite(File, L) when is_pid(File), is_list(L) ->
    pwrite_int(File, L, 0);
pwrite(#file_descriptor{module = Module} = Handle, L) when is_list(L) ->
    Module:pwrite(Handle, L);
pwrite(_, _) ->
    {error, badarg}.

pwrite_int(_File, [], _R) ->
    ok;
pwrite_int(File, [{At, Bytes} | T], R) ->
    case pwrite(File, At, Bytes) of
	ok ->
	    pwrite_int(File, T, R+1);
	{error, Reason} ->
	    {error, {R, Reason}}
    end;
pwrite_int(_, _, _) ->
    {error, badarg}.

pwrite(File, At, Bytes) when is_pid(File) ->
    R = file_request(File, {pwrite, At, Bytes}),
    wait_file_reply(File, R);
pwrite(#file_descriptor{module = Module} = Handle, Offs, Bytes) ->
    Module:pwrite(Handle, Offs, Bytes);
pwrite(_, _, _) ->
    {error, badarg}.

datasync(File) when is_pid(File) ->
    R = file_request(File, datasync),
    wait_file_reply(File, R);
datasync(#file_descriptor{module = Module} = Handle) ->
    Module:datasync(Handle);
datasync(_) ->
    {error, badarg}.

sync(File) when is_pid(File) ->
    R = file_request(File, sync),
    wait_file_reply(File, R);
sync(#file_descriptor{module = Module} = Handle) ->
    Module:sync(Handle);
sync(_) ->
    {error, badarg}.

position(File, At) when is_pid(File) ->
    R = file_request(File, {position,At}),
    wait_file_reply(File, R);
position(#file_descriptor{module = Module} = Handle, At) ->
    Module:position(Handle, At);
position(_, _) ->
    {error, badarg}.

truncate(File) when is_pid(File) ->
    R = file_request(File, truncate),
    wait_file_reply(File, R);
truncate(#file_descriptor{module = Module} = Handle) ->
    Module:truncate(Handle);
truncate(_) ->
    {error, badarg}.

copy(Source, Dest) ->
    copy_int(Source, Dest, infinity).

copy(Source, Dest, Length) 
  when is_integer(Length), Length >= 0;
       is_atom(Length) ->
    copy_int(Source, Dest, Length);
copy(_, _, _) ->
    {error, badarg}.

%% Here we know that Length is either an atom or an integer >= 0
%% (by the way, atoms > integers)
%%
%% Copy between open files. 
copy_int(Source, Dest, Length) 
  when is_pid(Source), is_pid(Dest);
       is_pid(Source), is_record(Dest, file_descriptor);
       is_record(Source, file_descriptor), is_pid(Dest) ->
    copy_opened_int(Source, Dest, Length, 0);
%% Copy between open raw files, both handled by the same module
copy_int(#file_descriptor{module = Module} = Source,
	 #file_descriptor{module = Module} = Dest,
	 Length) ->
    Module:copy(Source, Dest, Length);
%% Copy between open raw files of different modules
copy_int(#file_descriptor{} = Source, 
	 #file_descriptor{} = Dest, Length) ->
    copy_opened_int(Source, Dest, Length, 0);
%% Copy between filenames, let the server do the copy
copy_int({SourceName, SourceOpts}, {DestName, DestOpts}, Length) 
  when is_list(SourceOpts), is_list(DestOpts) ->
    check_and_call(copy, 
		   [file_name(SourceName), SourceOpts,
		    file_name(DestName), DestOpts,
		    Length]);
%% Filename -> open file; must open Source and do client copy
copy_int({SourceName, SourceOpts}, Dest, Length) 
  when is_list(SourceOpts), is_pid(Dest);
       is_list(SourceOpts), is_record(Dest, file_descriptor) ->
    case file_name(SourceName) of
	{error, _} = Error ->
	    Error;
	Source ->
	    case open(Source, [read | SourceOpts]) of
		{ok, Handle} ->
		    Result = copy_opened_int(Handle, Dest, Length, 0),
		    close(Handle),
		    Result;
		{error, _} = Error ->
		    Error
	    end
    end;
%% Open file -> filename; must open Dest and do client copy
copy_int(Source, {DestName, DestOpts}, Length)
  when is_pid(Source), is_list(DestOpts);
       is_record(Source, file_descriptor), is_list(DestOpts) ->
    case file_name(DestName) of
	{error, _} = Error ->
	    Error;
	Dest ->
	    case open(Dest, [write | DestOpts]) of
		{ok, Handle} ->
		    Result = copy_opened_int(Source, Handle, Length, 0),
		    close(Handle),
		    Result;
		{error, _} = Error ->
		    Error
	    end
    end;
%%
%% That was all combinations of {Name, Opts} tuples
%% and open files. At least one of Source and Dest has
%% to be a bare filename.
%%
%% If Source is not a bare filename; Dest must be
copy_int(Source, Dest, Length) 
  when is_pid(Source);
       is_record(Source, file_descriptor) ->
    copy_int(Source, {Dest, []}, Length);
copy_int({_SourceName, SourceOpts} = Source, Dest, Length) 
  when is_list(SourceOpts) ->
    copy_int(Source, {Dest, []}, Length);
%% If Dest is not a bare filename; Source must be
copy_int(Source, Dest, Length) 
  when is_pid(Dest);
       is_record(Dest, file_descriptor) ->
    copy_int({Source, []}, Dest, Length);
copy_int(Source, {_DestName, DestOpts} = Dest, Length) 
  when is_list(DestOpts) ->
    copy_int({Source, []}, Dest, Length);
%% Both must be bare filenames. If they are not,
%% the filename check in the copy operation will yell.
copy_int(Source, Dest, Length) ->
    copy_int({Source, []}, {Dest, []}, Length).



copy_opened(Source, Dest, Length)
  when is_integer(Length), Length >= 0;
       is_atom(Length) ->
    copy_opened_int(Source, Dest, Length);
copy_opened(_, _, _) ->
    {error, badarg}.

%% Here we know that Length is either an atom or an integer >= 0
%% (by the way, atoms > integers)

copy_opened_int(Source, Dest, Length)
  when is_pid(Source), is_pid(Dest) ->
    copy_opened_int(Source, Dest, Length, 0);
copy_opened_int(Source, Dest, Length)
  when is_pid(Source), is_record(Dest, file_descriptor) ->
    copy_opened_int(Source, Dest, Length, 0);
copy_opened_int(Source, Dest, Length)
  when is_record(Source, file_descriptor), is_pid(Dest) ->
    copy_opened_int(Source, Dest, Length, 0);
copy_opened_int(Source, Dest, Length)
  when is_record(Source, file_descriptor), is_record(Dest, file_descriptor) ->
    copy_opened_int(Source, Dest, Length, 0);
copy_opened_int(_, _, _) ->
    {error, badarg}.

%% Here we know that Source and Dest are handles to open files, Length is
%% as above, and Copied is an integer >= 0

%% Copy loop in client process
copy_opened_int(_, _, Length, Copied) when Length =< 0 -> % atom() > integer()
    {ok, Copied};
copy_opened_int(Source, Dest, Length, Copied) ->
    N = if Length > 65536 -> 65536; true -> Length end, % atom() > integer() !
    case read(Source, N) of
	{ok, Data} ->
	    M = if is_binary(Data) -> byte_size(Data);
		   is_list(Data)   -> length(Data)
		end,
	    case write(Dest, Data) of
		ok ->
		    if M < N ->
			    %% Got less than asked for - must be end of file
			    {ok, Copied+M};
		       true ->
			    %% Decrement Length (might be an atom (infinity))
			    NewLength = if is_atom(Length) -> Length;
					   true         -> Length-M
					end,
			    copy_opened_int(Source, Dest, NewLength, Copied+M)
		    end;
		{error, _} = Error ->
		    Error
	    end;
	eof ->
	    {ok, Copied};
	{error, _} = Error ->
	    Error
    end.


%% Special indirect pread function. Introduced for Dets.
%% Reads a header from pos 'Pos', the header is first a size encoded as
%% 32 bit big endian unsigned and then a position also encoded as
%% 32 bit big endian. Finally it preads the data from that pos and size 
%% in the file.

ipread_s32bu_p32bu(File, Pos, MaxSize) when is_pid(File) ->
    ipread_s32bu_p32bu_int(File, Pos, MaxSize);
ipread_s32bu_p32bu(#file_descriptor{module = Module} = Handle, Pos, MaxSize) ->
    Module:ipread_s32bu_p32bu(Handle, Pos, MaxSize);
ipread_s32bu_p32bu(_, _, _) ->
    {error, badarg}.

ipread_s32bu_p32bu_int(File, Pos, Infinity) when is_atom(Infinity) ->
    ipread_s32bu_p32bu_int(File, Pos, (1 bsl 31)-1);
ipread_s32bu_p32bu_int(File, Pos, MaxSize) 
  when is_integer(MaxSize), MaxSize >= 0 ->
    if
	MaxSize < (1 bsl 31) ->
	    case pread(File, Pos, 8) of
		{ok, Header} ->
		    ipread_s32bu_p32bu_2(File, Header, MaxSize);
		Error ->
		    Error
	    end;
	true ->
	    {error, einval}
    end;
ipread_s32bu_p32bu_int(_File, _Pos, _MaxSize) ->
    {error, badarg}.

ipread_s32bu_p32bu_2(_File, 
		     <<0:32/big-unsigned, Pos:32/big-unsigned>>,
		     _MaxSize) ->
    {ok, {0, Pos, eof}};
ipread_s32bu_p32bu_2(File, 
		     <<Size:32/big-unsigned, Pos:32/big-unsigned>>,
		     MaxSize) 
  when Size =< MaxSize ->
    case pread(File, Pos, Size) of
	{ok, Data} ->
	    {ok, {Size, Pos, Data}};
	eof ->
	    {ok, {Size, Pos, eof}};
	Error ->
	    Error
    end;
ipread_s32bu_p32bu_2(_File, 
		     <<_:8/binary>>,
		     _MaxSize) ->
    eof;
ipread_s32bu_p32bu_2(_File,
		     <<_/binary>>,
		     _MaxSize) ->
    eof;
ipread_s32bu_p32bu_2(File,
		    Header,
		    MaxSize) when is_list(Header) ->
    ipread_s32bu_p32bu_2(File, list_to_binary(Header), MaxSize).



%%%-----------------------------------------------------------------
%%% The following functions, built upon the other interface functions,
%%% provide a higher-lever interface to files.

consult(File) ->
    case open(File, [read]) of
	{ok, Fd} ->
	    R = consult_stream(Fd),
	    close(Fd),
	    R;
	Error ->
	    Error
    end.

path_consult(Path, File) ->
    case path_open(Path, File, [read]) of
	{ok, Fd, Full} ->
	    case consult_stream(Fd) of
		{ok, List} ->
		    close(Fd),
		    {ok, List, Full};
		E1 ->
		    close(Fd),
		    E1
	    end;
	E2 ->
	    E2
    end.

eval(File) ->
    eval(File, erl_eval:new_bindings()).

eval(File, Bs) ->
    case open(File, [read]) of
	{ok, Fd} ->
	    R = eval_stream(Fd, ignore, Bs),
	    close(Fd),
	    R;
	Error ->
	    Error
    end.

path_eval(Path, File) ->
    path_eval(Path, File, erl_eval:new_bindings()).

path_eval(Path, File, Bs) ->
    case path_open(Path, File, [read]) of
	{ok, Fd, Full} ->
	    case eval_stream(Fd, ignore, Bs) of
		ok ->
		    close(Fd),
		    {ok, Full};
		E1 ->
		    close(Fd),
		    E1
	    end;
	E2 ->
	    E2
    end.

script(File) ->
    script(File, erl_eval:new_bindings()).

script(File, Bs) ->
    case open(File, [read]) of
	{ok, Fd} ->
	    R = eval_stream(Fd, return, Bs),
	    close(Fd),
	    R;
	Error ->
	    Error
    end.

path_script(Path, File) ->
    path_script(Path, File, erl_eval:new_bindings()).

path_script(Path, File, Bs) ->
    case path_open(Path, File, [read]) of
	{ok,Fd,Full} ->
	    case eval_stream(Fd, return, Bs) of
		{ok,R} ->
		    close(Fd),
		    {ok, R, Full};
		E1 ->
		    close(Fd),
		    E1
	    end;
	E2 ->
	    E2
    end.
    

%% path_open(Paths, Filename, Mode) ->
%%	{ok,FileDescriptor,FullName}
%%	{error,Reason}
%%
%% Searches the Paths for file Filename which can be opened with Mode.
%% The path list is ignored if Filename contains an absolute path.

path_open(PathList, Name, Mode) ->
    case file_name(Name) of
	{error, _} = Error ->
	    Error;
	FileName ->
	    case filename:pathtype(FileName) of
		relative ->
		    path_open_first(PathList, FileName, Mode, enoent);
		_ ->
		    case open(Name, Mode) of
			{ok, Fd} ->
			    {ok, Fd, Name};
			Error ->
			    Error
		    end
	    end
    end.

change_mode(Name, Mode) 
  when is_integer(Mode) ->
    write_file_info(Name, #file_info{mode=Mode}).

change_owner(Name, OwnerId) 
  when is_integer(OwnerId) ->
    write_file_info(Name, #file_info{uid=OwnerId}).

change_owner(Name, OwnerId, GroupId) 
  when is_integer(OwnerId), is_integer(GroupId) ->
    write_file_info(Name, #file_info{uid=OwnerId, gid=GroupId}).

change_group(Name, GroupId) 
  when is_integer(GroupId) ->
    write_file_info(Name, #file_info{gid=GroupId}).

change_time(Name, Time) 
  when is_tuple(Time) ->
    write_file_info(Name, #file_info{mtime=Time}).

change_time(Name, Atime, Mtime) 
  when is_tuple(Atime), is_tuple(Mtime) ->
    write_file_info(Name, #file_info{atime=Atime, mtime=Mtime}).

%%%-----------------------------------------------------------------
%%% Helpers

consult_stream(Fd) ->
    consult_stream(Fd, 1, []).

consult_stream(Fd, Line, Acc) ->
    case io:read(Fd, '', Line) of
	{ok,Term,EndLine} ->
	    consult_stream(Fd, EndLine, [Term|Acc]);
	{error,Error,_Line} ->
	    {error,Error};
	{eof,_Line} ->
	    {ok,lists:reverse(Acc)}
    end.

eval_stream(Fd, Handling, Bs) ->
    eval_stream(Fd, Handling, 1, undefined, [], Bs).

eval_stream(Fd, H, Line, Last, E, Bs) ->
    eval_stream2(io:parse_erl_exprs(Fd, '', Line), Fd, H, Last, E, Bs).

eval_stream2({ok,Form,EndLine}, Fd, H, Last, E, Bs0) ->
    try erl_eval:exprs(Form, Bs0) of
	{value,V,Bs} ->
	    eval_stream(Fd, H, EndLine, {V}, E, Bs)
    catch Class:Reason ->
            Error = {EndLine,?MODULE,{Class,Reason,erlang:get_stacktrace()}},
	    eval_stream(Fd, H, EndLine, Last, [Error|E], Bs0)
    end;
eval_stream2({error,What,EndLine}, Fd, H, Last, E, Bs) ->
    eval_stream(Fd, H, EndLine, Last, [What | E], Bs);
eval_stream2({eof,EndLine}, _Fd, H, Last, E, _Bs) ->
    case {H, Last, E} of
	{return, {Val}, []} ->
	    {ok, Val};
	{return, undefined, E} ->
	    {error, hd(lists:reverse(E, [{EndLine,?MODULE,undefined_script}]))};
	{ignore, _, []} ->
	    ok;
	{_, _, [_|_] = E} ->
	    {error, hd(lists:reverse(E))}
    end.

path_open_first([Path|Rest], Name, Mode, LastError) ->
    case file_name(Path) of
	{error, _} = Error ->
	    Error;
	FilePath ->
	    FileName = filename:join(FilePath, Name),
	    case open(FileName, Mode) of
		{ok, Fd} ->
		    {ok, Fd, FileName};
		{error, enoent} ->
		    path_open_first(Rest, Name, Mode, LastError);
		Error ->
		    Error
	    end
    end;
path_open_first([], _Name, _Mode, LastError) ->
    {error, LastError}.

%%%-----------------------------------------------------------------
%%% Utility functions.

%% file_name(FileName)
%% 	Generates a flat file name from a deep list of atoms and 
%% 	characters (integers).

file_name(N) ->
    try 
        file_name_1(N)
    catch Reason ->
        {error, Reason}
    end.

file_name_1([C|T]) when is_integer(C), C > 0, C =< 255 ->
    [C|file_name_1(T)];
file_name_1([H|T]) ->
    file_name_1(H) ++ file_name_1(T);
file_name_1([]) ->
    [];
file_name_1(N) when is_atom(N) ->
    atom_to_list(N);
file_name_1(_) ->
    throw(badarg).

make_binary(Bin) when is_binary(Bin) ->
    Bin;
make_binary(List) ->
    %% Convert the list to a binary in order to avoid copying a list
    %% to the file server.
    try 
        erlang:iolist_to_binary(List)
    catch error:Reason ->
        {error, Reason}
    end.

mode_list(read) ->
    [read];
mode_list(write) ->
    [write];
mode_list(read_write) ->
    [read, write];
mode_list({binary, Mode}) when is_atom(Mode) ->
    [binary | mode_list(Mode)];
mode_list({character, Mode}) when is_atom(Mode) ->
    mode_list(Mode);
mode_list(_) ->
    [{error, badarg}].

%%-----------------------------------------------------------------
%% Functions for communicating with the file server

call(Command, Args) when is_list(Args) ->
    Pid = case whereis(serialiser) of
              undefined -> start_serialiser();
              Pid2      -> Pid2
          end,
    serialiser:submit(
      Pid,
      fun () ->
              gen_server:call(?FILE_SERVER, list_to_tuple([Command | Args]),
                              infinity)
      end).

start_serialiser() ->
    {ok, Pid} = supervisor:start_child(kernel_sup,
                                       {serialiser, {serialiser, start_link, []},
                                        permanent, 16#ffffffff, worker,
                                        [serialiser]}),
    true = register(serialiser, Pid),
    Pid.

check_and_call(Command, Args) when is_list(Args) ->
    case check_args(Args) of
	ok ->
	    call(Command, Args);
	Error ->
	    Error
    end.

check_args([{error, _}=Error|_Rest]) ->
    Error;
check_args([_Name|Rest]) ->
    check_args(Rest);
check_args([]) ->
    ok.

%%-----------------------------------------------------------------
%% Functions for communicating with a file io server.
%% The messages sent have the following formats:
%%
%%	{file_request,From,ReplyAs,Request}
%%	{file_reply,ReplyAs,Reply}

file_request(Io, Request) ->
    R = erlang:monitor(process, Io),
    Io ! {file_request,self(),Io,Request},
    R.

wait_file_reply(From, Ref) ->
    receive
	{file_reply,From,Reply} ->
	    erlang:demonitor(Ref),
	    receive {'DOWN', Ref, _, _, _} -> ok after 0 -> ok end,
	    %% receive {'EXIT', From, _} -> ok after 0 -> ok end,
	    Reply;
	{'DOWN', Ref, _, _, _} ->
	    %% receive {'EXIT', From, _} -> ok after 0 -> ok end,
	    {error, terminated}
    end.
