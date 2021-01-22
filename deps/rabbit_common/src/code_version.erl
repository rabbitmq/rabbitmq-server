%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%
-module(code_version).

-export([update/1, get_otp_version/0]).

%%----------------------------------------------------------------------------
%% API
%%----------------------------------------------------------------------------

%%----------------------------------------------------------------------------
%% @doc Reads the abstract code of the given `Module`, modifies it to adapt to
%% the current Erlang version, compiles and loads the result.
%% This function finds the current Erlang version and then selects the function
%% call for that version, removing all other versions declared in the original
%% beam file. `code_version:update/1` is triggered by the module itself the
%% first time an affected function is called.
%%
%% The purpose of this functionality is to support the new time API introduced
%% in ERTS 7.0, while providing compatibility with previous versions.
%%
%% `Module` must contain an attribute `erlang_version_support` containing a list of
%% tuples:
%%
%% {ErlangVersion, [{OriginalFunction, Arity, PreErlangVersionFunction,
%%                   PostErlangVersionFunction}]}
%%
%% All these new functions may be exported, and implemented as follows:
%%
%% OriginalFunction() ->
%%    code_version:update(?MODULE),
%%    ?MODULE:OriginalFunction().
%%
%% PostErlangVersionFunction() ->
%%    %% implementation using new time API
%%    ..
%%
%% PreErlangVersionFunction() ->
%%    %% implementation using fallback solution
%%    ..
%%
%% CAUTION: Make sure that all functions in the module are patched this
%% way! If you have "regular" functions, you might hit a race condition
%% between the unload of the old module and the load of the patched
%% module. If all functions are patched, loading will be serialized,
%% thanks to a lock acquired by `code_version`. However, if you have
%% regular functions, any call to them will bypass that lock and the old
%% code will be reloaded from disk. This will kill the process trying to
%% patch the module.
%%
%% end
%%----------------------------------------------------------------------------
-spec update(atom()) -> ok | no_return().
update(Module) ->
    AbsCode = get_abs_code(Module),
    Forms = replace_forms(Module, get_otp_version(), AbsCode),
    Code = compile_forms(Forms),
    load_code(Module, Code).

%%----------------------------------------------------------------------------
%% Internal functions
%%----------------------------------------------------------------------------
load_code(Module, Code) ->
    LockId = {{?MODULE, Module}, self()},
    FakeFilename = "Loaded by rabbit_common",
    global:set_lock(LockId, [node()]),
    case code:which(Module) of
        FakeFilename ->
            ok;
        _ ->
            unload(Module),
            case code:load_binary(Module, FakeFilename, Code) of
                {module, _}     -> ok;
                {error, Reason} -> throw({cannot_load, Module, Reason})
            end
    end,
    global:del_lock(LockId, [node()]),
    ok.

unload(Module) ->
    code:soft_purge(Module),
    code:delete(Module).

compile_forms(Forms) ->
    case compile:forms(Forms, [debug_info, return_errors]) of
        {ok, _ModName, Code} ->
            Code;
        {ok, _ModName, Code, _Warnings} ->
            Code;
        Error ->
            throw({cannot_compile_forms, Error})
    end.

get_abs_code(Module) ->
    get_forms(get_object_code(Module)).

get_object_code(Module) ->
    case code:get_object_code(Module) of
        {_Mod, Code, _File} ->
            Code;
        error ->
            throw({not_found, Module})
    end.

get_forms(Code) ->
    case beam_lib:chunks(Code, [abstract_code]) of
        {ok, {_, [{abstract_code, {raw_abstract_v1, Forms}}]}} ->
            Forms;
        {ok, {Module, [{abstract_code, no_abstract_code}]}} ->
            throw({no_abstract_code, Module});
        {error, beam_lib, Reason} ->
            throw({no_abstract_code, Reason})
    end.

get_otp_version() ->
    Version = erlang:system_info(otp_release),
    case re:run(Version, "^[0-9][0-9]", [{capture, first, list}]) of
        {match, [V]} ->
            list_to_integer(V);
        _ ->
            %% Could be anything below R17, we are not interested
            0
    end.

get_original_pairs(VersionSupport) ->
    [{Orig, Arity} || {Orig, Arity, _Pre, _Post} <- VersionSupport].

get_delete_pairs(true, VersionSupport) ->
    [{Pre, Arity} || {_Orig, Arity, Pre, _Post} <- VersionSupport];
get_delete_pairs(false, VersionSupport) ->
    [{Post, Arity} || {_Orig, Arity, _Pre, Post} <- VersionSupport].

get_rename_pairs(true, VersionSupport) ->
    [{Post, Arity} || {_Orig, Arity, _Pre, Post} <- VersionSupport];
get_rename_pairs(false, VersionSupport) ->
    [{Pre, Arity} || {_Orig, Arity, Pre, _Post} <- VersionSupport].

%% Pairs of {Renamed, OriginalName} functions
get_name_pairs(true, VersionSupport) ->
    [{{Post, Arity}, Orig} || {Orig, Arity, _Pre, Post} <- VersionSupport];
get_name_pairs(false, VersionSupport) ->
    [{{Pre, Arity}, Orig} || {Orig, Arity, Pre, _Post} <- VersionSupport].

delete_abstract_functions(ToDelete) ->
    fun(Tree, Function) ->
            case lists:member(Function, ToDelete) of
                true ->
                    erl_syntax:comment(["Deleted unused function"]);
                false ->
                    Tree
            end
    end.

rename_abstract_functions(ToRename, ToName) ->
    fun(Tree, Function) ->
            case lists:member(Function, ToRename) of
                true ->
                    FunctionName = proplists:get_value(Function, ToName),
                    erl_syntax:function(
                      erl_syntax:atom(FunctionName),
                      erl_syntax:function_clauses(Tree));
                false ->
                    Tree
            end
    end.

replace_forms(Module, ErlangVersion, AbsCode) ->
    %% Obtain attribute containing the list of functions that must be updated
    Attr = Module:module_info(attributes),
    VersionSupport = proplists:get_value(erlang_version_support, Attr),
    {Pre, Post} = lists:splitwith(fun({Version, _Pairs}) ->
                                          Version > ErlangVersion
                                  end, VersionSupport),
    %% Replace functions in two passes: replace for Erlang versions > current
    %% first, Erlang versions =< current afterwards.
    replace_version_forms(
      true, replace_version_forms(false, AbsCode, get_version_functions(Pre)),
      get_version_functions(Post)).

get_version_functions(List) ->
    lists:append([Pairs || {_Version, Pairs} <- List]).

replace_version_forms(IsPost, AbsCode, VersionSupport) ->
    %% Get pairs of {Function, Arity} for the triggering functions, which
    %% are also the final function names.
    Original = get_original_pairs(VersionSupport),
    %% Get pairs of {Function, Arity} for the unused version
    ToDelete = get_delete_pairs(IsPost, VersionSupport),
    %% Delete original functions (those that trigger the code update) and
    %% the unused version ones
    DeleteFun = delete_abstract_functions(ToDelete ++ Original),
    AbsCode0 = replace_function_forms(AbsCode, DeleteFun),
    %% Get pairs of {Function, Arity} for the current version which must be
    %% renamed
    ToRename = get_rename_pairs(IsPost, VersionSupport),
    %% Get paris of {Renamed, OriginalName} functions
    ToName = get_name_pairs(IsPost, VersionSupport),
    %% Rename versioned functions with their final name
    RenameFun = rename_abstract_functions(ToRename, ToName),
    AbsCode1 = replace_function_forms(AbsCode0, RenameFun),
    %% Adjust `-dialyzer` attribute.
    AbsCode2 = fix_dialyzer_attribute(AbsCode1, ToDelete, ToName),
    %% Remove exports of all versioned functions
    remove_exports(AbsCode2, ToDelete ++ ToRename).

replace_function_forms(AbsCode, Fun) ->
    ReplaceFunction =
        fun(Tree) ->
                Function = erl_syntax_lib:analyze_function(Tree),
                Fun(Tree, Function)
        end,
    Filter = fun(Tree) ->
                     case erl_syntax:type(Tree) of
                         function -> ReplaceFunction(Tree);
                         _Other -> Tree
                     end
             end,
    fold_syntax_tree(Filter, AbsCode).

fix_dialyzer_attribute(AbsCode, ToDelete, ToName) ->
    FixDialyzer =
        fun(Tree) ->
                case erl_syntax_lib:analyze_attribute(Tree) of
                    {dialyzer, {_, Value}} ->
                        FixedValue = fix_dialyzer_attribute_value(Value,
                                                                  ToDelete,
                                                                  ToName),
                        rebuild_dialyzer({dialyzer, FixedValue});
                    _ ->
                        Tree
                end
        end,
    Filter = fun(Tree) ->
                     case erl_syntax:type(Tree) of
                         attribute -> FixDialyzer(Tree);
                         _         -> Tree
                     end
             end,
    fold_syntax_tree(Filter, AbsCode).

fix_dialyzer_attribute_value(Info, ToDelete, ToName)
  when is_list(Info) ->
    lists:map(
      fun(I) ->
              fix_dialyzer_attribute_value(I, ToDelete, ToName)
      end,
      Info);
fix_dialyzer_attribute_value({Warn, FunList}, ToDelete, ToName) ->
    FixedFunList = fix_dialyzer_attribute_funlist(FunList, ToDelete, ToName),
    {Warn, FixedFunList};
fix_dialyzer_attribute_value(Info, _, _)
  when is_atom(Info) ->
    Info.

fix_dialyzer_attribute_funlist(FunList, ToDelete, ToName)
  when is_list(FunList) ->
    lists:filtermap(
      fun(I) ->
              case fix_dialyzer_attribute_funlist(I, ToDelete, ToName) of
                  [] -> false;
                  R  -> {true, R}
              end
      end,
      FunList);
fix_dialyzer_attribute_funlist({FunName, Arity} = Fun,
                               ToDelete, ToName)
  when is_atom(FunName) andalso is_integer(Arity) andalso Arity >= 0 ->
    remove_or_rename(Fun, ToDelete, ToName);
fix_dialyzer_attribute_funlist(FunList, _, _) ->
    FunList.

remove_or_rename(Fun, ToDelete, ToName) ->
    case lists:member(Fun, ToDelete) of
        true ->
            [];
        false ->
            case proplists:get_value(Fun, ToName) of
                undefined -> Fun;
                NewName   -> setelement(1, Fun, NewName)
            end
    end.

rebuild_dialyzer({dialyzer, Value}) ->
    erl_syntax:attribute(
      erl_syntax:atom(dialyzer),
      [rebuild_dialyzer_value(Value)]).

rebuild_dialyzer_value(Value) when is_list(Value) ->
    erl_syntax:list(
      [rebuild_dialyzer_value(V) || V <- Value]);
rebuild_dialyzer_value({Warn, FunList}) ->
    erl_syntax:tuple(
      [rebuild_dialyzer_warn(Warn),
       rebuild_dialyzer_funlist(FunList)]);
rebuild_dialyzer_value(Warn) when is_atom(Warn) ->
    rebuild_dialyzer_warn(Warn).

rebuild_dialyzer_warn(Warn) when is_list(Warn) ->
    erl_syntax:list(
      [rebuild_dialyzer_warn(W) || W <- Warn]);
rebuild_dialyzer_warn(Warn) when is_atom(Warn) ->
    erl_syntax:atom(Warn).

rebuild_dialyzer_funlist(FunList) when is_list(FunList) ->
    erl_syntax:list(
      [rebuild_dialyzer_funlist({N, A}) || {N, A} <- FunList]);
rebuild_dialyzer_funlist({FunName, Arity}) ->
    erl_syntax:tuple([erl_syntax:atom(FunName), erl_syntax:integer(Arity)]).

filter_export_pairs(Info, ToDelete) ->
    lists:filter(fun(Pair) ->
                         not lists:member(Pair, ToDelete)
                 end, Info).

remove_exports(AbsCode, ToDelete) ->
    RemoveExports =
        fun(Tree) ->
                case erl_syntax_lib:analyze_attribute(Tree) of
                    {export, Info} ->
                        Remaining = filter_export_pairs(Info, ToDelete),
                        rebuild_export(Remaining);
                    _Other -> Tree
                end
        end,
    Filter = fun(Tree) ->
                     case erl_syntax:type(Tree) of
                         attribute -> RemoveExports(Tree);
                         _Other -> Tree
                     end
             end,
    fold_syntax_tree(Filter, AbsCode).

rebuild_export(Args) ->
    erl_syntax:attribute(
      erl_syntax:atom(export),
      [erl_syntax:list(
         [erl_syntax:arity_qualifier(erl_syntax:atom(N),
                                     erl_syntax:integer(A))
          || {N, A} <- Args])]).

fold_syntax_tree(Filter, Forms) ->
    Tree = erl_syntax:form_list(Forms),
    NewTree = erl_syntax_lib:map(Filter, Tree),
    erl_syntax:revert_forms(NewTree).
