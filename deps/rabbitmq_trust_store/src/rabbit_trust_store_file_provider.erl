%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rabbit_trust_store_file_provider).

-include_lib("kernel/include/file.hrl").

-behaviour(rabbit_trust_store_certificate_provider).

-export([list_certs/1, list_certs/2, load_cert/3]).

-define(DIRECTORY_OR_FILE_NAME_EXISTS, eexist).

-type cert_id() :: {FileName :: string(),
                    ChangeTime :: integer(),
                    Hash :: integer()}.

-spec list_certs(Config :: list())
    -> no_change | {ok, [{cert_id(), list()}], State}
    when State :: nostate.
list_certs(Config) ->
    Path = directory_path(Config),
    Certs = list_certs_0(Path),
    {ok, Certs, nostate}.

-spec list_certs(Config :: list(), State)
    -> no_change | {ok, [{cert_id(), list()}], State}
    when State :: nostate.
list_certs(Config, _) ->
    list_certs(Config).

-spec load_cert(cert_id(), list(), Config :: list())
    -> {ok, Cert :: public_key:der_encoded()}.
load_cert({FileName, _, _}, _, Config) ->
    Path = directory_path(Config),
    Cert = extract_cert(Path, FileName),
    _ = rabbit_log:info(
      "trust store: loading certificate '~s'", [FileName]),
    {ok, Cert}.

extract_cert(Path, FileName) ->
    Absolute = filename:join(Path, FileName),
    scan_then_parse(Absolute).

scan_then_parse(Filename) when is_list(Filename) ->
    {ok, Bin} = file:read_file(Filename),
    [{'Certificate', Data, not_encrypted}] = public_key:pem_decode(Bin),
    Data.

list_certs_0(Path) ->
    {ok, FileNames} = file:list_dir(Path),
    lists:map(
        fun(FileName) ->
            AbsName = filename:absname(FileName, Path),
            CertId = {FileName,
                      modification_time(AbsName),
                      file_content_hash(AbsName)},
            {CertId, [{name, FileName}]}
        end,
        FileNames).

modification_time(Path) ->
    {ok, Info} = file:read_file_info(Path, [{time, posix}]),
    Info#file_info.mtime.

file_content_hash(Path) ->
    {ok, Data} = file:read_file(Path),
    erlang:phash2(Data).

directory_path(Config) ->
    directory_path(Config, default_directory()).

directory_path(Config, Default) ->
    Path = case proplists:get_value(directory, Config) of
        undefined ->
            Default;
        V when is_binary(V) ->
            binary_to_list(V);
        V when is_list(V) ->
            V
    end,
    ok = ensure_directory(Path),
    Path.

default_directory() ->
    %% Dismantle the directory tree: first the table & meta-data
    %% directory, then the Mesia database directory, finally the node
    %% directory where we will place the default whitelist in `Full`.
    Table  = filename:split(rabbit_mnesia:dir()),
    Node   = lists:sublist(Table, length(Table) - 2),
    Full   = Node ++ ["trust_store", "whitelist"],
    filename:join(Full).

ensure_directory(Path) ->
    ok = ensure_parent_directories(Path),
    case file:make_dir(Path) of
        {error, ?DIRECTORY_OR_FILE_NAME_EXISTS} ->
            true = filelib:is_dir(Path),
            ok;
        ok ->
            ok
    end.

ensure_parent_directories(Path) ->
    filelib:ensure_dir(Path).

