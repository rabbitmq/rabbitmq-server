-module(rabbit_trust_store_file_provider).

-include_lib("kernel/include/file.hrl").

-behaviour(rabbit_trust_store_certificate_provider).

-export([list_certs/1, list_certs/2, load_cert/3]).

-define(DIRECTORY_OR_FILE_NAME_EXISTS, eexist).

-record(directory_state, {
    directory_path,
    directory_change_time}).

-type cert_id() :: {FileName :: string(), ChangeTime :: integer()}.

-spec list_certs(Config :: list())
    -> no_change | {ok, [{cert_id(), map()}], State}
    when State :: #directory_state{}.
list_certs(Config) ->
    Path = directory_path(Config),
    NewChangeTime = modification_time(Path),
    Certs = list_certs_0(Path),
    {ok, Certs, #directory_state{directory_path = Path,
                                 directory_change_time = NewChangeTime}}.

-spec list_certs(Config :: list(), State)
    -> no_change | {ok, [{cert_id(), map()}], State}
    when State :: #directory_state{}.
list_certs(Config, #directory_state{directory_path = DirPath,
                                    directory_change_time = ChangeTime}) ->
    Path = directory_path(Config, DirPath),
    NewChangeTime = modification_time(Path),
    case NewChangeTime > ChangeTime of
        false ->
            no_change;
        true  ->
            Certs = list_certs_0(Path),
            {ok, Certs, #directory_state{directory_path = Path,
                                         directory_change_time = NewChangeTime}}
    end.

-spec load_cert(cert_id(), map(), Config :: list())
    -> {ok, Cert :: public_key:der_encoded()}.
load_cert({FileName, _}, #{}, Config) ->
    Path = directory_path(Config),
    Cert = extract_cert(Path, FileName),
    rabbit_log:info(
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
            CertId = {FileName, modification_time(AbsName)},
            {CertId, #{name => FileName}}
        end,
        FileNames).

modification_time(Path) ->
    {ok, Info} = file:read_file_info(Path, [{time, posix}]),
    Info#file_info.mtime.

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
    Mnesia = lists:droplast(Table),
    Node   = lists:droplast(Mnesia),
    Full = Node ++ ["trust_store", "whitelist"],
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

