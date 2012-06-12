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
%% Copyright (c) 2011-2012 VMware, Inc.  All rights reserved.
%%

-module(rabbit_file).

-include_lib("kernel/include/file.hrl").

-export([is_file/1, is_dir/1, file_size/1, ensure_dir/1, wildcard/2, list_dir/1]).
-export([read_term_file/1, write_term_file/2, write_file/2, write_file/3]).
-export([append_file/2, ensure_parent_dirs_exist/1]).
-export([rename/2, delete/1, recursive_delete/1, recursive_copy/2]).
-export([lock_file/1]).

%%----------------------------------------------------------------------------

-ifdef(use_specs).

-type(ok_or_error() :: rabbit_types:ok_or_error(any())).

-spec(is_file/1 :: ((file:filename())) -> boolean()).
-spec(is_dir/1 :: ((file:filename())) -> boolean()).
-spec(file_size/1 :: ((file:filename())) -> non_neg_integer()).
-spec(ensure_dir/1 :: ((file:filename())) -> ok_or_error()).
-spec(wildcard/2 :: (string(), file:filename()) -> [file:filename()]).
-spec(list_dir/1 :: (file:filename()) -> rabbit_types:ok_or_error2(
                                           [file:filename()], any())).
-spec(read_term_file/1 ::
        (file:filename()) -> {'ok', [any()]} | rabbit_types:error(any())).
-spec(write_term_file/2 :: (file:filename(), [any()]) -> ok_or_error()).
-spec(write_file/2 :: (file:filename(), iodata()) -> ok_or_error()).
-spec(write_file/3 :: (file:filename(), iodata(), [any()]) -> ok_or_error()).
-spec(append_file/2 :: (file:filename(), string()) -> ok_or_error()).
-spec(ensure_parent_dirs_exist/1 :: (string()) -> 'ok').
-spec(rename/2 ::
        (file:filename(), file:filename()) -> ok_or_error()).
-spec(delete/1 :: ([file:filename()]) -> ok_or_error()).
-spec(recursive_delete/1 ::
        ([file:filename()])
        -> rabbit_types:ok_or_error({file:filename(), any()})).
-spec(recursive_copy/2 ::
        (file:filename(), file:filename())
        -> rabbit_types:ok_or_error({file:filename(), file:filename(), any()})).
-spec(lock_file/1 :: (file:filename()) -> rabbit_types:ok_or_error('eexist')).

-endif.

%%----------------------------------------------------------------------------

is_file(File) ->
    case read_file_info(File) of
        {ok, #file_info{type=regular}}   -> true;
        {ok, #file_info{type=directory}} -> true;
        _                                -> false
    end.

is_dir(Dir) -> is_dir_internal(read_file_info(Dir)).

is_dir_no_handle(Dir) -> is_dir_internal(prim_file:read_file_info(Dir)).

is_dir_internal({ok, #file_info{type=directory}}) -> true;
is_dir_internal(_)                                -> false.

file_size(File) ->
    case read_file_info(File) of
        {ok, #file_info{size=Size}} -> Size;
        _                           -> 0
    end.

ensure_dir(File) -> with_fhc_handle(fun () -> ensure_dir_internal(File) end).

ensure_dir_internal("/")  ->
    ok;
ensure_dir_internal(File) ->
    Dir = filename:dirname(File),
    case is_dir_no_handle(Dir) of
        true  -> ok;
        false -> ensure_dir_internal(Dir),
                 prim_file:make_dir(Dir)
    end.

wildcard(Pattern, Dir) ->
    {ok, Files} = list_dir(Dir),
    {ok, RE} = re:compile(Pattern, [anchored]),
    [File || File <- Files, match =:= re:run(File, RE, [{capture, none}])].

list_dir(Dir) -> with_fhc_handle(fun () -> prim_file:list_dir(Dir) end).

read_file_info(File) ->
    with_fhc_handle(fun () -> prim_file:read_file_info(File) end).

with_fhc_handle(Fun) ->
    ok = file_handle_cache:obtain(),
    try Fun()
    after ok = file_handle_cache:release()
    end.

read_term_file(File) ->
    try
        {ok, Data} = with_fhc_handle(fun () -> prim_file:read_file(File) end),
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

write_term_file(File, Terms) ->
    write_file(File, list_to_binary([io_lib:format("~w.~n", [Term]) ||
                                        Term <- Terms])).

write_file(Path, Data) -> write_file(Path, Data, []).

%% write_file/3 and make_binary/1 are both based on corresponding
%% functions in the kernel/file.erl module of the Erlang R14B02
%% release, which is licensed under the EPL. That implementation of
%% write_file/3 does not do an fsync prior to closing the file, hence
%% the existence of this version. APIs are otherwise identical.
write_file(Path, Data, Modes) ->
    Modes1 = [binary, write | (Modes -- [binary, write])],
    case make_binary(Data) of
        Bin when is_binary(Bin) ->
            with_fhc_handle(
              fun () -> case prim_file:open(Path, Modes1) of
                            {ok, Hdl}      -> try prim_file:write(Hdl, Bin) of
                                                  ok -> prim_file:sync(Hdl);
                                                  {error, _} = E -> E
                                              after
                                                  prim_file:close(Hdl)
                                              end;
                            {error, _} = E -> E
                        end
              end);
        {error, _} = E -> E
    end.

make_binary(Bin) when is_binary(Bin) ->
    Bin;
make_binary(List) ->
    try
        iolist_to_binary(List)
    catch error:Reason ->
            {error, Reason}
    end.


append_file(File, Suffix) ->
    case read_file_info(File) of
        {ok, FInfo}     -> append_file(File, FInfo#file_info.size, Suffix);
        {error, enoent} -> append_file(File, 0, Suffix);
        Error           -> Error
    end.

append_file(_, _, "") ->
    ok;
append_file(File, 0, Suffix) ->
    with_fhc_handle(fun () ->
                            case prim_file:open([File, Suffix], [append]) of
                                {ok, Fd} -> prim_file:close(Fd);
                                Error    -> Error
                            end
                    end);
append_file(File, _, Suffix) ->
    case with_fhc_handle(fun () -> prim_file:read_file(File) end) of
        {ok, Data} -> write_file([File, Suffix], Data, [append]);
        Error      -> Error
    end.

ensure_parent_dirs_exist(Filename) ->
    case ensure_dir(Filename) of
        ok              -> ok;
        {error, Reason} ->
            throw({error, {cannot_create_parent_dirs, Filename, Reason}})
    end.

rename(Old, New) -> with_fhc_handle(fun () -> prim_file:rename(Old, New) end).

delete(File) -> with_fhc_handle(fun () -> prim_file:delete(File) end).

recursive_delete(Files) ->
    with_fhc_handle(
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
lock_file(Path) ->
    case is_file(Path) of
        true  -> {error, eexist};
        false -> with_fhc_handle(
                   fun () -> {ok, Lock} = prim_file:open(Path, [write]),
                             ok = prim_file:close(Lock)
                   end)
    end.
