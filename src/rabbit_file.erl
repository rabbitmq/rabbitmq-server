%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at https://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and
%% limitations under the License.
%%
%% The Original Code is RabbitMQ.
%%
%% The Initial Developer of the Original Code is GoPivotal, Inc.
%% Copyright (c) 2011-2020 VMware, Inc. or its affiliates.  All rights reserved.
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

-import(file_handle_cache, [with_handle/1, with_handle/2]).

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

is_dir_no_handle(Dir) -> is_dir_internal(prim_file:read_file_info(Dir)).

is_dir_internal({ok, #file_info{type=directory}}) -> true;
is_dir_internal(_)                                -> false.

-spec file_size((file:filename())) -> non_neg_integer().

file_size(File) ->
    case read_file_info(File) of
        {ok, #file_info{size=Size}} -> Size;
        _                           -> 0
    end.

-spec ensure_dir((file:filename())) -> ok_or_error().

ensure_dir(File) -> with_handle(fun () -> ensure_dir_internal(File) end).

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

list_dir(Dir) -> with_handle(fun () -> prim_file:list_dir(Dir) end).

read_file_info(File) ->
    with_handle(fun () -> prim_file:read_file_info(File) end).

-spec read_term_file
        (file:filename()) -> {'ok', [any()]} | rabbit_types:error(any()).

read_term_file(File) ->
    try
        {ok, Data} = with_handle(fun () -> prim_file:read_file(File) end),
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
            with_handle(
              fun () ->
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
              end)
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
    with_handle(fun () ->
                        case prim_file:open([File, Suffix], [append]) of
                            {ok, Fd} -> prim_file:close(Fd);
                            Error    -> Error
                        end
                end);
append_file(File, _, Suffix) ->
    case with_handle(2, fun () ->
                                file:copy(File, {[File, Suffix], [append]})
                        end) of
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

rename(Old, New) -> with_handle(fun () -> prim_file:rename(Old, New) end).

-spec delete([file:filename()]) -> ok_or_error().

delete(File) -> with_handle(fun () -> prim_file:delete(File) end).

-spec recursive_delete([file:filename()]) ->
          rabbit_types:ok_or_error({file:filename(), any()}).

recursive_delete(Files) ->
    with_handle(
      fun () -> lists:foldl(fun (Path,  ok) -> recursive_delete1(Path);
                                (_Path, {error, _Err} = Error) -> Error
                            end, ok, Files)
      end).

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
        false -> with_handle(
                   fun () -> {ok, Lock} = prim_file:open(Path, [write]),
                             ok = prim_file:close(Lock)
                   end)
    end.

-spec filename_as_a_directory(file:filename()) -> file:filename().

filename_as_a_directory(FileName) ->
    case lists:last(FileName) of
        "/" ->
            FileName;
        _ ->
            FileName ++ "/"
    end.
