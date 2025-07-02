%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2025 Broadcom. All Rights Reserved. The term “Broadcom” refers to Broadcom Inc. and/or its subsidiaries. All rights reserved.
%%

-module(rabbit_file).

-include_lib("kernel/include/file.hrl").

-export([is_file/1, is_dir/1, file_size/1, ensure_dir/1, wildcard/2, list_dir/1]).
-export([read_term_file/1, write_term_file/2, write_file/2, write_file/3]).
-export([append_file/2, ensure_parent_dirs_exist/1]).
-export([rename/2, delete/1, recursive_delete/1, recursive_copy/2]).
-export([lock_file/1]).
-export([read_file_info/1]).
-export([filename_as_a_directory/1]).
-export([filename_to_binary/1, binary_to_filename/1]).
-export([open_eventually/2]).

-define(TMP_EXT, ".tmp").

%%----------------------------------------------------------------------------

-type ok_or_error() :: rabbit_types:ok_or_error(any()).

%%----------------------------------------------------------------------------

-spec is_file((file:filename())) -> boolean().

is_file(File) ->
    case read_file_info(File) of
        {ok, #file_info{type=regular}}   -> true;
        {ok, #file_info{type=directory}} -> true;
        _                                -> false
    end.

-spec is_dir((file:filename())) -> boolean().

is_dir(Dir) -> is_dir_internal(read_file_info(Dir)).

is_dir_no_handle(Dir) -> is_dir_internal(file:read_file_info(Dir, [raw])).

is_dir_internal({ok, #file_info{type=directory}}) -> true;
is_dir_internal(_)                                -> false.

-spec file_size((file:filename())) -> non_neg_integer().

file_size(File) ->
    case read_file_info(File) of
        {ok, #file_info{size=Size}} -> Size;
        _                           -> 0
    end.

-spec ensure_dir((file:filename())) -> ok_or_error().

ensure_dir(File) -> ensure_dir_internal(File).

ensure_dir_internal("/")  ->
    ok;
ensure_dir_internal(File) ->
    Dir = filename:dirname(File),
    case is_dir_no_handle(Dir) of
        true  -> ok;
        false -> ensure_dir_internal(Dir),
                 prim_file:make_dir(Dir)
    end.

-spec wildcard(string(), file:filename()) -> [file:filename()].

wildcard(Pattern, Dir) ->
    case list_dir(Dir) of
        {ok, Files} -> {ok, RE} = re:compile(Pattern, [anchored]),
                       [File || File <- Files,
                                match =:= re:run(File, RE, [{capture, none}])];
        {error, _}  -> []
    end.

-spec list_dir(file:filename()) ->
          rabbit_types:ok_or_error2([file:filename()], any()).

list_dir(Dir) -> prim_file:list_dir(Dir).

read_file_info(File) ->
    file:read_file_info(File, [raw]).

-spec read_term_file
        (file:filename()) -> {'ok', [any()]} | rabbit_types:error(any()).

read_term_file(File) ->
    try
        %% @todo OTP-27+ has file:read_file(File, [raw]).
        F = fun() ->
                    {ok, FInfo} = file:read_file_info(File, [raw]),
                    {ok, Fd} = file:open(File, [read, raw, binary]),
                    try
                        file:read(Fd, FInfo#file_info.size)
                    after
                        file:close(Fd)
                    end
            end,
        {ok, Data} = F(),
        {ok, Tokens, _} = erl_scan:string(binary_to_list(Data)),
        TokenGroups = group_tokens(Tokens),
        {ok, [begin
                  {ok, Term} = erl_parse:parse_term(Tokens1),
                  Term
              end || Tokens1 <- TokenGroups]}
    catch
        error:{badmatch, Error} -> Error
    end.

group_tokens(Ts) -> [lists:reverse(G) || G <- group_tokens([], Ts)].

group_tokens([], [])                    -> [];
group_tokens(Cur, [])                   -> [Cur];
group_tokens(Cur, [T = {dot, _} | Ts])  -> [[T | Cur] | group_tokens([], Ts)];
group_tokens(Cur, [T | Ts])             -> group_tokens([T | Cur], Ts).

-spec write_term_file(file:filename(), [any()]) -> ok_or_error().

write_term_file(File, Terms) ->
    write_file(File, list_to_binary([io_lib:format("~w.~n", [Term]) ||
                                        Term <- Terms])).

-spec write_file(file:filename(), iodata()) -> ok_or_error().

write_file(Path, Data) -> write_file(Path, Data, []).

-spec write_file(file:filename(), iodata(), [any()]) -> ok_or_error().

write_file(Path, Data, Modes) ->
    Modes1 = [binary, write | (Modes -- [binary, write])],
    case make_binary(Data) of
        Bin when is_binary(Bin) -> write_file1(Path, Bin, Modes1);
        {error, _} = E          -> E
    end.

%% make_binary/1 is based on the corresponding function in the
%% kernel/file.erl module of the Erlang R14B02 release, which is
%% licensed under the EPL.

make_binary(Bin) when is_binary(Bin) ->
    Bin;
make_binary(List) ->
    try
        iolist_to_binary(List)
    catch error:Reason ->
            {error, Reason}
    end.

write_file1(Path, Bin, Modes) ->
    try
        with_synced_copy(Path, Modes,
                         fun (Hdl) ->
                                 ok = prim_file:write(Hdl, Bin)
                         end)
    catch
        error:{badmatch, Error} -> Error;
            _:{error, Error}    -> {error, Error}
    end.

with_synced_copy(Path, Modes, Fun) ->
    case lists:member(append, Modes) of
        true ->
            {error, append_not_supported, Path};
        false ->
            Bak = Path ++ ?TMP_EXT,
            case prim_file:open(Bak, Modes) of
                {ok, Hdl} ->
                    try
                        Result = Fun(Hdl),
                        ok = prim_file:sync(Hdl),
                        ok = prim_file:rename(Bak, Path),
                        Result
                    after
                        prim_file:close(Hdl)
                    end;
                {error, _} = E -> E
            end
    end.

%% TODO the semantics of this function are rather odd. But see bug 25021.

-spec append_file(file:filename(), string()) -> ok_or_error().

append_file(File, Suffix) ->
    case read_file_info(File) of
        {ok, FInfo}     -> append_file(File, FInfo#file_info.size, Suffix);
        {error, enoent} -> append_file(File, 0, Suffix);
        Error           -> Error
    end.

append_file(_, _, "") ->
    ok;
append_file(File, 0, Suffix) ->
    case prim_file:open([File, Suffix], [append]) of
        {ok, Fd} -> prim_file:close(Fd);
        Error    -> Error
    end;
append_file(File, _, Suffix) ->
    case file:copy(File, {[File, Suffix], [append]}) of
        {ok, _BytesCopied} -> ok;
        Error              -> Error
    end.

-spec ensure_parent_dirs_exist(string()) -> 'ok'.

ensure_parent_dirs_exist(Filename) ->
    case ensure_dir(Filename) of
        ok              -> ok;
        {error, Reason} ->
            throw({error, {cannot_create_parent_dirs, Filename, Reason}})
    end.

-spec rename(file:filename(), file:filename()) -> ok_or_error().

rename(Old, New) -> prim_file:rename(Old, New).

-spec delete([file:filename()]) -> ok_or_error().

delete(File) -> prim_file:delete(File).

-spec recursive_delete([file:filename()]) ->
          rabbit_types:ok_or_error({file:filename(), any()}).

recursive_delete(Files) ->
    lists:foldl(fun (Path,  ok) -> recursive_delete1(Path);
                    (_Path, {error, _Err} = Error) -> Error
                end, ok, Files).

recursive_delete1(Path) ->
    case is_dir_no_handle(Path) and not(is_symlink_no_handle(Path)) of
        false -> case prim_file:delete(Path) of
                     ok              -> ok;
                     {error, enoent} -> ok; %% Path doesn't exist anyway
                     {error, Err}    -> {error, {Path, Err}}
                 end;
        true  -> case prim_file:list_dir(Path) of
                     {ok, FileNames} ->
                         case lists:foldl(
                                fun (FileName, ok) ->
                                        recursive_delete1(
                                          filename:join(Path, FileName));
                                    (_FileName, Error) ->
                                        Error
                                end, ok, FileNames) of
                             ok ->
                                 case prim_file:del_dir(Path) of
                                     ok           -> ok;
                                     {error, ebusy}  -> ok; %% Can't delete a mount point
                                     {error, Err} -> {error, {Path, Err}}
                                 end;
                             {error, _Err} = Error ->
                                 Error
                         end;
                     {error, Err} ->
                         {error, {Path, Err}}
                 end
    end.

is_symlink_no_handle(File) ->
    case prim_file:read_link(File) of
        {ok, _} -> true;
        _       -> false
    end.

-spec recursive_copy(file:filename(), file:filename()) ->
          rabbit_types:ok_or_error({file:filename(), file:filename(), any()}).

recursive_copy(Src, Dest) ->
    %% Note that this uses the 'file' module and, hence, shouldn't be
    %% run on many processes at once.
    case is_dir(Src) of
        false -> case file:copy(Src, Dest) of
                     {ok, _Bytes}    -> ok;
                     {error, enoent} -> ok; %% Path doesn't exist anyway
                     {error, Err}    -> {error, {Src, Dest, Err}}
                 end;
        true  -> case file:list_dir(Src) of
                     {ok, FileNames} ->
                         case file:make_dir(Dest) of
                             ok ->
                                 lists:foldl(
                                   fun (FileName, ok) ->
                                           recursive_copy(
                                             filename:join(Src, FileName),
                                             filename:join(Dest, FileName));
                                       (_FileName, Error) ->
                                           Error
                                   end, ok, FileNames);
                             {error, Err} ->
                                 {error, {Src, Dest, Err}}
                         end;
                     {error, Err} ->
                         {error, {Src, Dest, Err}}
                 end
    end.

%% TODO: When we stop supporting Erlang prior to R14, this should be
%% replaced with file:open [write, exclusive]

-spec lock_file(file:filename()) -> rabbit_types:ok_or_error('eexist').

lock_file(Path) ->
    case is_file(Path) of
        true  -> {error, eexist};
        false -> {ok, Lock} = prim_file:open(Path, [write]),
                 ok = prim_file:close(Lock)
    end.

-spec filename_as_a_directory(file:filename()) -> file:filename().

filename_as_a_directory(FileName) ->
    case lists:last(FileName) of
        "/" ->
            FileName;
        _ ->
            FileName ++ "/"
    end.

-spec filename_to_binary(file:filename()) ->
    binary().
filename_to_binary(Name) when is_list(Name) ->
    case unicode:characters_to_binary(Name, unicode, file:native_name_encoding()) of
        Bin when is_binary(Bin) ->
            Bin;
        Other ->
            erlang:error(Other)
    end.

-spec binary_to_filename(binary()) ->
    file:filename().
binary_to_filename(Bin) when is_binary(Bin) ->
    case unicode:characters_to_list(Bin, file:native_name_encoding()) of
        Name when is_list(Name) ->
            Name;
        Other ->
            erlang:error(Other)
    end.

%% On Windows the file may be in "DELETE PENDING" state following
%% its deletion (when the last message was acked). A subsequent
%% open may fail with an {error,eacces}. In that case we wait 10ms
%% and retry up to 3 times.

-spec open_eventually(File, Modes) -> {ok, IoDevice} | {error, Reason} when
      File :: Filename | iodata(),
      Filename :: file:name_all(),
      Modes :: [file:mode() | ram | directory],
      IoDevice :: file:io_device(),
      Reason :: file:posix() | badarg | system_limit.

open_eventually(File, Modes) ->
    open_eventually(File, Modes, 3).

open_eventually(_, _, 0) ->
    {error, eacces};
open_eventually(File, Modes, N) ->
    case file:open(File, Modes) of
        OK = {ok, _} ->
            OK;
        %% When the current write file was recently deleted it
        %% is possible on Windows to get an {error,eacces}.
        %% Sometimes Windows sets the files to "DELETE PENDING"
        %% state and delays deletion a bit. So we wait 10ms and
        %% try again up to 3 times.
        {error, eacces} ->
            timer:sleep(10),
            open_eventually(File, Modes, N - 1)
    end.
