-module(trust_store_list_handler).
-behaviour(cowboy_handler).

-include_lib("kernel/include/file.hrl").

-export([init/2]).
-export([terminate/3]).

init(Req, State) ->
    {ok, Directory} = application:get_env(trust_store_http, directory),
    case list_files(Directory) of
        {ok, Files}  -> respond(Files, Req, State);
        {error, Err} -> respond_error(Err, Req, State)
    end.

terminate(_Reason, _Req, _State) ->
    ok.

respond(Files, Req, State) ->
    ResponseBody = json_encode(Files),
    Headers = #{<<"content-type">> => <<"application/json">>},
    Req2 = cowboy_req:reply(200, Headers, ResponseBody, Req),
    {ok, Req2, State}.

respond_error(Reason, Req, State) ->
    Error = io_lib:format("Error listing certificates ~tp", [Reason]),
    logger:log(error, "~ts", [Error]),
    Req2 = cowboy_req:reply(500, #{}, iolist_to_binary(Error), Req),
    {ok, Req2, State}.

json_encode(Files) ->
    Map = #{certificates => [ #{id   => cert_id(FileName, FileDate, FileHash),
                                path => cert_path(FileName)}
                              || {FileName, FileDate, FileHash} <- Files ]},
    thoas:encode(Map).

cert_id(FileName, FileDate, FileHash) ->
    iolist_to_binary(io_lib:format("~ts:~tp:~tp", [FileName, FileDate, FileHash])).

cert_path(FileName) ->
    iolist_to_binary(["/certs/", FileName]).

list_files(Directory) ->
    case file:list_dir(Directory) of
        {ok, FileNames} ->
            PemFiles = [ FileName || FileName <- FileNames,
                                     filename:extension(FileName) == ".pem" ],
            {ok, lists:map(
                    fun(FileName) ->
                        FullName = filename:join(Directory, FileName),
                        {ok, Mtime} = modification_time(FullName, posix),
                        Hash = file_content_hash(FullName),
                        {FileName, Mtime, Hash}
                    end,
                    PemFiles)};
        {error, Err} -> {error, Err}
    end.

modification_time(FileName, Type) ->
    case file:read_file_info(FileName, [{time, Type}]) of
        {ok, #file_info{mtime = Mtime}} -> {ok, Mtime};
        {error, Reason} -> {error, Reason}
    end.

file_content_hash(Path) ->
    {ok, Data} = file:read_file(Path),
    erlang:phash2(Data).
