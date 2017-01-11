-module(rabbit_trust_store_file_provider).

-include_lib("kernel/include/file.hrl").

-behaviour(rabbit_trust_store_certificate_provider).

-export([list_certs/2, get_cert_data/3]).

-record(directory_state, {directory_change_time}).

list_certs(Config, nostate) ->
    Path = directory_path(Config),
    NewChangeTime = modification_time(Path),
    list_certs_0(Path, NewChangeTime);
list_certs(Config, #directory_state{directory_change_time = ChangeTime}) ->
    Path = directory_path(Config),
    NewChangeTime = modification_time(Path),
    case NewChangeTime > ChangeTime of
        false ->
            no_change;
        true  ->
            list_certs_0(Path, NewChangeTime)
    end.

get_cert_data({FileName, _}, no_attributes, Config) ->
    Path = directory_path(Config),
    try extract_cert(Path, FileName) of
        Cert ->
            rabbit_log:info(
              "trust store: loading certificate '~s'", [FileName]),
            {ok, Cert}
    catch
        _:Err ->
            {error, Err}
    end.

extract_cert(Path, FileName) ->
    Absolute = filename:join(Path, FileName),
    scan_then_parse(Absolute).

scan_then_parse(Filename) when is_list(Filename) ->
    {ok, Bin} = file:read_file(Filename),
    [{'Certificate', Data, not_encrypted}] = public_key:pem_decode(Bin),
    public_key:pkix_decode_cert(Data, otp).

list_certs_0(Path, NewChangeTime) ->
    {ok, FileNames} = file:list_dir(Path),
    Certs = lists:map(
        fun(FileName) ->
            AbsName = filename:absname(FileName, Path),
            CertId = {FileName, modification_time(AbsName)},
            {CertId, no_attributes}
        end,
        FileNames),
    {ok, Certs, #directory_state{directory_change_time = NewChangeTime}}.

modification_time(Path) ->
    {ok, Info} = file:read_file_info(Path, [{time, posix}]),
    Info#file_info.mtime.

directory_path(Config) ->
    {directory, Path} = lists:keyfind(directory, 1, Config),
    Path.
