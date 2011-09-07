%% This is a version of 'filelib' from R14B03, which uses 'file2'
%% instead of 'file'.  Use this module when you expect a large number
%% of concurrent file operations.

%%
%% %CopyrightBegin%
%%
%% Copyright Ericsson AB 1997-2011. All Rights Reserved.
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

-module(filelib2).

%% File utilities.

%% Avoid warning for local function error/1 clashing with autoimported BIF.
-compile({no_auto_import,[error/1]}).
-export([wildcard/1, wildcard/2, is_dir/1, is_file/1, is_regular/1, 
	 compile_wildcard/1]).
-export([fold_files/5, last_modified/1, file_size/1, ensure_dir/1]).

-export([wildcard/3, is_dir/2, is_file/2, is_regular/2]).
-export([fold_files/6, last_modified/2, file_size/2]).

-include_lib("kernel/include/file.hrl").

-define(HANDLE_ERROR(Expr),
	try
	    Expr
	catch
	    error:{badpattern,_}=UnUsUalVaRiAbLeNaMe ->
		%% Get the stack backtrace correct.
		erlang:error(UnUsUalVaRiAbLeNaMe)
	end).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

wildcard(Pattern) when is_list(Pattern) ->
    ?HANDLE_ERROR(do_wildcard(Pattern, file)).

wildcard(Pattern, Cwd) when is_list(Pattern), (is_list(Cwd) or is_binary(Cwd)) ->
    ?HANDLE_ERROR(do_wildcard(Pattern, Cwd, file));
wildcard(Pattern, Mod) when is_list(Pattern), is_atom(Mod) ->
    ?HANDLE_ERROR(do_wildcard(Pattern, Mod)).

-spec wildcard(file:name(), file:name(), atom()) -> [file:filename()].
wildcard(Pattern, Cwd, Mod)
  when is_list(Pattern), (is_list(Cwd) or is_binary(Cwd)), is_atom(Mod) ->
    ?HANDLE_ERROR(do_wildcard(Pattern, Cwd, Mod)).

is_dir(Dir) ->
    do_is_dir(Dir, file).

is_dir(Dir, Mod) when is_atom(Mod) ->
    do_is_dir(Dir, Mod).

is_file(File) ->
    do_is_file(File, file).

is_file(File, Mod) when is_atom(Mod) ->
    do_is_file(File, Mod).

is_regular(File) ->
    do_is_regular(File, file).
    
is_regular(File, Mod) when is_atom(Mod) ->
    do_is_regular(File, Mod).
    
fold_files(Dir, RegExp, Recursive, Fun, Acc) ->
    do_fold_files(Dir, RegExp, Recursive, Fun, Acc, file).

fold_files(Dir, RegExp, Recursive, Fun, Acc, Mod) when is_atom(Mod) ->
    do_fold_files(Dir, RegExp, Recursive, Fun, Acc, Mod).

last_modified(File) ->
    do_last_modified(File, file).

-spec last_modified(file:name(), atom()) -> file:date_time() | 0.
last_modified(File, Mod) when is_atom(Mod) ->
    do_last_modified(File, Mod).

file_size(File) ->
    do_file_size(File, file).

-spec file_size(file:name(), atom()) -> non_neg_integer().
file_size(File, Mod) when is_atom(Mod) ->
    do_file_size(File, Mod).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

do_wildcard(Pattern, Mod) when is_list(Pattern) ->
    do_wildcard_comp(do_compile_wildcard(Pattern), Mod).

do_wildcard_comp({compiled_wildcard,{exists,File}}, Mod) ->
    case eval_read_file_info(File, Mod) of
	{ok,_} -> [File];
	_ -> []
    end;
do_wildcard_comp({compiled_wildcard,[Base|Rest]}, Mod) ->
    do_wildcard_1([Base], Rest, Mod).

do_wildcard(Pattern, Cwd, Mod) when is_list(Pattern), (is_list(Cwd) or is_binary(Cwd)) ->
    do_wildcard_comp(do_compile_wildcard(Pattern), Cwd, Mod).

do_wildcard_comp({compiled_wildcard,{exists,File}}, Cwd, Mod) ->
    case eval_read_file_info(filename:absname(File, Cwd), Mod) of
	{ok,_} -> [File];
	_ -> []
    end;
do_wildcard_comp({compiled_wildcard,[current|Rest]}, Cwd0, Mod) ->
    {Cwd,PrefixLen} = case filename:join([Cwd0]) of
	      Bin when is_binary(Bin) -> {Bin,byte_size(Bin)+1};
	      Other -> {Other,length(Other)+1}
	  end,		%Slash away redundant slashes.
    [
     if 
	 is_binary(N) ->
	     <<_:PrefixLen/binary,Res/binary>> = N,
	     Res;
	 true ->
	     lists:nthtail(PrefixLen, N)
     end || N <- do_wildcard_1([Cwd], Rest, Mod)];
do_wildcard_comp({compiled_wildcard,[Base|Rest]}, _Cwd, Mod) ->
    do_wildcard_1([Base], Rest, Mod).

do_is_dir(Dir, Mod) ->
    case eval_read_file_info(Dir, Mod) of
	{ok, #file_info{type=directory}} ->
	    true;
	_ ->
	    false
    end.

do_is_file(File, Mod) ->
    case eval_read_file_info(File, Mod) of
	{ok, #file_info{type=regular}} ->
	    true;
	{ok, #file_info{type=directory}} ->
	    true;
        _ ->
            false
    end.

do_is_regular(File, Mod) ->
    case eval_read_file_info(File, Mod) of
	{ok, #file_info{type=regular}} ->
	    true;
        _ ->
            false
    end.

%% fold_files(Dir, RegExp, Recursive, Fun, AccIn).

%% folds the function Fun(F, Acc) -> Acc1 over
%%   all files <F> in <Dir> that match the regular expression <RegExp>
%%   If <Recursive> is true all sub-directories to <Dir> are processed

do_fold_files(Dir, RegExp, Recursive, Fun, Acc, Mod) ->
    {ok, Re1} = re:compile(RegExp,[unicode]),
    do_fold_files1(Dir, Re1, RegExp, Recursive, Fun, Acc, Mod).

do_fold_files1(Dir, RegExp, OrigRE, Recursive, Fun, Acc, Mod) ->
    case eval_list_dir(Dir, Mod) of
	{ok, Files} -> do_fold_files2(Files, Dir, RegExp, OrigRE,
				      Recursive, Fun, Acc, Mod);
	{error, _}  -> Acc
    end.

%% OrigRE is not to be compiled as it's for non conforming filenames,
%% i.e. for filenames that does not comply to the current encoding, which should
%% be very rare. We use it only in those cases and do not want to precompile.
do_fold_files2([], _Dir, _RegExp, _OrigRE, _Recursive, _Fun, Acc, _Mod) -> 
    Acc;
do_fold_files2([File|T], Dir, RegExp, OrigRE, Recursive, Fun, Acc0, Mod) ->
    FullName = filename:join(Dir, File),
    case do_is_regular(FullName, Mod) of
	true  ->
	    case (catch re:run(File, if is_binary(File) -> OrigRE; 
					true -> RegExp end, 
			       [{capture,none}])) of
		match  -> 
		    Acc = Fun(FullName, Acc0),
		    do_fold_files2(T, Dir, RegExp, OrigRE, Recursive, Fun, Acc, Mod);
		{'EXIT',_} ->
		    do_fold_files2(T, Dir, RegExp, OrigRE, Recursive, Fun, Acc0, Mod);
		nomatch ->
		    do_fold_files2(T, Dir, RegExp, OrigRE, Recursive, Fun, Acc0, Mod)
	    end;
	false ->
	    case Recursive andalso do_is_dir(FullName, Mod) of
		true ->
		    Acc1 = do_fold_files1(FullName, RegExp, OrigRE, Recursive,
					  Fun, Acc0, Mod),
		    do_fold_files2(T, Dir, RegExp, OrigRE, Recursive, Fun, Acc1, Mod);
		false ->
		    do_fold_files2(T, Dir, RegExp, OrigRE, Recursive, Fun, Acc0, Mod)
	    end
    end.

do_last_modified(File, Mod) ->
    case eval_read_file_info(File, Mod) of
	{ok, Info} ->
	    Info#file_info.mtime;
	_ ->
	    0
    end.

do_file_size(File, Mod) ->
    case eval_read_file_info(File, Mod) of
	{ok, Info} ->
	    Info#file_info.size;
	_ ->
	    0
    end.

%%----------------------------------------------------------------------
%% +type ensure_dir(X) -> ok | {error, Reason}.
%% +type X = filename() | dirname()
%% ensures that the directory name required to create D exists

ensure_dir("/") ->
    ok;
ensure_dir(F) ->
    Dir = filename:dirname(F),
    case do_is_dir(Dir, file) of
	true ->
	    ok;
	false ->
	    ensure_dir(Dir),
	    case file2:make_dir(Dir) of
		{error,eexist}=EExist ->
		    case do_is_dir(Dir, file) of
			true ->
			    ok;
			false ->
			    EExist
		    end;
		Err ->
		    Err
	    end
    end.


%%%
%%% Pattern matching using a compiled wildcard.
%%%

do_wildcard_1(Files, Pattern, Mod) ->
    do_wildcard_2(Files, Pattern, [], Mod).

do_wildcard_2([File|Rest], Pattern, Result, Mod) ->
    do_wildcard_2(Rest, Pattern, do_wildcard_3(File, Pattern, Result, Mod), Mod);
do_wildcard_2([], _, Result, _Mod) ->
    Result.

do_wildcard_3(Base, [Pattern|Rest], Result, Mod) ->
    case do_list_dir(Base, Mod) of
	{ok, Files0} ->
	    Files = lists:sort(Files0),
	    Matches = wildcard_4(Pattern, Files, Base, []),
	    do_wildcard_2(Matches, Rest, Result, Mod);
	_ ->
	    Result
    end;
do_wildcard_3(Base, [], Result, _Mod) ->
    [Base|Result].

wildcard_4(Pattern, [File|Rest], Base, Result) when is_binary(File) ->
    case wildcard_5(Pattern, binary_to_list(File)) of
	true ->
	    wildcard_4(Pattern, Rest, Base, [join(Base, File)|Result]);
	false ->
	    wildcard_4(Pattern, Rest, Base, Result)
    end;
wildcard_4(Pattern, [File|Rest], Base, Result) ->
    case wildcard_5(Pattern, File) of
	true ->
	    wildcard_4(Pattern, Rest, Base, [join(Base, File)|Result]);
	false ->
	    wildcard_4(Pattern, Rest, Base, Result)
    end;
wildcard_4(_Patt, [], _Base, Result) ->
    Result.

wildcard_5([question|Rest1], [_|Rest2]) ->
    wildcard_5(Rest1, Rest2);
wildcard_5([accept], _) ->
    true;
wildcard_5([star|Rest], File) ->
    do_star(Rest, File);
wildcard_5([{one_of, Ordset}|Rest], [C|File]) ->
    case ordsets:is_element(C, Ordset) of
	true  -> wildcard_5(Rest, File);
	false -> false
    end;
wildcard_5([{alt, Alts}], File) ->
    do_alt(Alts, File);
wildcard_5([C|Rest1], [C|Rest2]) when is_integer(C) ->
    wildcard_5(Rest1, Rest2);
wildcard_5([X|_], [Y|_]) when is_integer(X), is_integer(Y) ->
    false;
wildcard_5([], []) ->
    true;
wildcard_5([], [_|_]) ->
    false;
wildcard_5([_|_], []) ->
    false.

do_star(Pattern, [X|Rest]) ->
    case wildcard_5(Pattern, [X|Rest]) of
	true  -> true;
	false -> do_star(Pattern, Rest)
    end;
do_star(Pattern, []) ->
    wildcard_5(Pattern, []).

do_alt([Alt|Rest], File) ->
    case wildcard_5(Alt, File) of
	true  -> true;
	false -> do_alt(Rest, File)
    end;
do_alt([], _File) ->
    false.

do_list_dir(current, Mod) -> eval_list_dir(".", Mod);
do_list_dir(Dir, Mod) ->     eval_list_dir(Dir, Mod).

join(current, File) -> File;
join(Base, File) -> filename:join(Base, File).

	    
%%% Compiling a wildcard.

compile_wildcard(Pattern) ->
    ?HANDLE_ERROR(do_compile_wildcard(Pattern)).

do_compile_wildcard(Pattern) ->
    {compiled_wildcard,compile_wildcard_1(Pattern)}.

compile_wildcard_1(Pattern) ->
    [Root|Rest] = filename:split(Pattern),
    case filename:pathtype(Root) of
	relative ->
	    compile_wildcard_2([Root|Rest], current);
	_ ->
	    compile_wildcard_2(Rest, [Root])
    end.

compile_wildcard_2([Part|Rest], Root) ->
    case compile_part(Part) of
	Part ->
	    compile_wildcard_2(Rest, join(Root, Part));
	Pattern ->
	    compile_wildcard_3(Rest, [Pattern,Root])
    end;
compile_wildcard_2([], Root) -> {exists,Root}.

compile_wildcard_3([Part|Rest], Result) ->
    compile_wildcard_3(Rest, [compile_part(Part)|Result]);
compile_wildcard_3([], Result) ->
    lists:reverse(Result).

compile_part(Part) ->
    compile_part(Part, false, []).

compile_part_to_sep(Part) ->
    compile_part(Part, true, []).

compile_part([], true, _) ->
    error(missing_delimiter);
compile_part([$,|Rest], true, Result) ->
    {ok, $,, lists:reverse(Result), Rest};
compile_part([$}|Rest], true, Result) ->
    {ok, $}, lists:reverse(Result), Rest};
compile_part([$?|Rest], Upto, Result) ->
    compile_part(Rest, Upto, [question|Result]);
compile_part([$*], Upto, Result) ->
    compile_part([], Upto, [accept|Result]);
compile_part([$*|Rest], Upto, Result) ->
    compile_part(Rest, Upto, [star|Result]);
compile_part([$[|Rest], Upto, Result) ->
    case compile_charset(Rest, ordsets:new()) of
	{ok, Charset, Rest1} ->
	    compile_part(Rest1, Upto, [Charset|Result]);
	error ->
	    compile_part(Rest, Upto, [$[|Result])
    end;
compile_part([${|Rest], Upto, Result) ->
    case compile_alt(Rest) of
	{ok, Alt} ->
	    lists:reverse(Result, [Alt]);
	error ->
	    compile_part(Rest, Upto, [${|Result])
    end;
compile_part([X|Rest], Upto, Result) ->
    compile_part(Rest, Upto, [X|Result]);
compile_part([], _Upto, Result) ->
    lists:reverse(Result).

compile_charset([$]|Rest], Ordset) ->
    compile_charset1(Rest, ordsets:add_element($], Ordset));
compile_charset([$-|Rest], Ordset) ->
    compile_charset1(Rest, ordsets:add_element($-, Ordset));
compile_charset([], _Ordset) ->
    error;
compile_charset(List, Ordset) ->
    compile_charset1(List, Ordset).

compile_charset1([Lower, $-, Upper|Rest], Ordset) when Lower =< Upper ->
    compile_charset1(Rest, compile_range(Lower, Upper, Ordset));
compile_charset1([$]|Rest], Ordset) ->
    {ok, {one_of, Ordset}, Rest};
compile_charset1([X|Rest], Ordset) ->
    compile_charset1(Rest, ordsets:add_element(X, Ordset));
compile_charset1([], _Ordset) ->
    error.
    
compile_range(Lower, Current, Ordset) when Lower =< Current ->
    compile_range(Lower, Current-1, ordsets:add_element(Current, Ordset));
compile_range(_, _, Ordset) ->
    Ordset.

compile_alt(Pattern) ->
    compile_alt(Pattern, []).

compile_alt(Pattern, Result) ->
    case compile_part_to_sep(Pattern) of
	{ok, $,, AltPattern, Rest} ->
	    compile_alt(Rest, [AltPattern|Result]);
	{ok, $}, AltPattern, Rest} ->
	    NewResult = [AltPattern|Result],
	    RestPattern = compile_part(Rest),
	    {ok, {alt, [Alt++RestPattern || Alt <- NewResult]}};
	Pattern ->
	    error
    end.

error(Reason) ->
    erlang:error({badpattern,Reason}).

eval_read_file_info(File, file) ->
    file2:read_file_info(File);
eval_read_file_info(File, erl_prim_loader) ->
    case erl_prim_loader:read_file_info(File) of
	error -> {error, erl_prim_loader};
	Res-> Res
    end;
eval_read_file_info(File, Mod) ->
    Mod:read_file_info(File).

eval_list_dir(Dir, file) ->
    file2:list_dir(Dir);
eval_list_dir(Dir, erl_prim_loader) ->
    case erl_prim_loader:list_dir(Dir) of
	error -> {error, erl_prim_loader};
	Res-> Res
    end;
eval_list_dir(Dir, Mod) ->
    Mod:list_dir(Dir).
