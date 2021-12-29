%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(rfc6455_client).

-export([new/2, new/3, new/4, new/5, open/1, recv/1, send/2, close/1, close/2]).

-record(state, {host, port, addr, path, ppid, socket, data, phase, transport}).

%% --------------------------------------------------------------------------

new(WsUrl, PPid) ->
    new(WsUrl, PPid, undefined, [], <<>>).

new(WsUrl, PPid, AuthInfo) ->
    new(WsUrl, PPid, AuthInfo, [], <<>>).

new(WsUrl, PPid, AuthInfo, Protocols) ->
    new(WsUrl, PPid, AuthInfo, Protocols, <<>>).

new(WsUrl, PPid, AuthInfo, Protocols, TcpPreface) ->
    crypto:start(),
    application:ensure_all_started(ssl),
    {Transport, Url} = case WsUrl of
        "ws://" ++ Rest -> {gen_tcp, Rest};
        "wss://" ++ SslRest -> {ssl, SslRest}
    end,
    [Addr, Path] = split("/", Url, 1),
    [Host0, MaybePort] = split(":", Addr, 1, empty),
    Host = case inet:parse_ipv4_address(Host0) of
        {ok, IP} -> IP;
        _ -> Host0
    end,
    Port = case MaybePort of
               empty -> 80;
               V     -> {I, ""} = string:to_integer(V), I
           end,
    State = #state{host = Host,
                   port = Port,
                   addr = Addr,
                   path = "/" ++ Path,
                   ppid = PPid,
                   transport = Transport},
    spawn_link(fun () ->
                  start_conn(State, AuthInfo, Protocols, TcpPreface)
          end).

open(WS) ->
    receive
        {rfc6455, open, WS, Opts} ->
            {ok, Opts};
        {rfc6455, close, WS, R} ->
            {close, R}
    end.

recv(WS) ->
    receive
        {rfc6455, recv, WS, Payload} ->
            {ok, Payload};
        {rfc6455, recv_binary, WS, Payload} ->
            {binary, Payload};
        {rfc6455, close, WS, R} ->
            {close, R}
    end.

send(WS, IoData) ->
    WS ! {send, IoData},
    ok.

close(WS) ->
    close(WS, {1000, ""}).

close(WS, WsReason) ->
    WS ! {close, WsReason},
    receive
        {rfc6455, close, WS, R} ->
            {close, R}
    end.


%% --------------------------------------------------------------------------

start_conn(State = #state{transport = Transport}, AuthInfo, Protocols, TcpPreface) ->
    {ok, Socket} = case TcpPreface of
        <<>> ->
            Transport:connect(State#state.host, State#state.port,
                              [binary,
                               {packet, 0}]);
        _ ->
            {ok, Socket0} = gen_tcp:connect(State#state.host, State#state.port,
                                            [binary,
                                             {packet, 0}]),
            gen_tcp:send(Socket0, TcpPreface),
            case Transport of
                gen_tcp -> {ok, Socket0};
                ssl -> Transport:connect(Socket0, [])
            end
    end,

    AuthHd = case AuthInfo of
        undefined -> "";
        _ ->
            Login    = proplists:get_value(login, AuthInfo),
            Passcode = proplists:get_value(passcode, AuthInfo),
            "Authorization: Basic "
                ++ base64:encode_to_string(Login ++ ":" ++ Passcode)
                ++ "\r\n"
    end,

    ProtocolHd = case Protocols of
        [] -> "";
        _  -> "Sec-Websocket-Protocol: " ++ string:join(Protocols, ", ") ++ "\r\n"
    end,

    Key = base64:encode_to_string(crypto:strong_rand_bytes(16)),
    Transport:send(Socket,
        "GET " ++ State#state.path ++ " HTTP/1.1\r\n" ++
        "Host: " ++ State#state.addr ++ "\r\n" ++
        "Upgrade: websocket\r\n" ++
        "Connection: Upgrade\r\n" ++
        AuthHd ++
        ProtocolHd ++
        "Sec-WebSocket-Key: " ++ Key ++ "\r\n" ++
        "Origin: null\r\n" ++
        "Sec-WebSocket-Version: 13\r\n\r\n"),

    loop(State#state{socket = Socket,
                     data   = <<>>,
                     phase = opening}).

do_recv(State = #state{phase = opening, ppid = PPid, data = Data}) ->
    case split("\r\n\r\n", binary_to_list(Data), 1, empty) of
        [_Http, empty] -> State;
        [Http, Data1]   ->
            %% TODO: don't ignore http response data, verify key
            PPid ! {rfc6455, open, self(), [{http_response, Http}]},
            State#state{phase = open,
                        data = Data1}
    end;
do_recv(State = #state{phase = Phase, data = Data, socket = Socket, transport = Transport, ppid = PPid})
  when Phase =:= open orelse Phase =:= closing ->
    R = case Data of
            <<F:1, _:3, O:4, 0:1, L:7, Payload:L/binary, Rest/binary>>
              when L < 126 ->
                {F, O, Payload, Rest};

            <<F:1, _:3, O:4, 0:1, 126:7, L2:16, Payload:L2/binary, Rest/binary>> ->
                {F, O, Payload, Rest};

            <<F:1, _:3, O:4, 0:1, 127:7, L2:64, Payload:L2/binary, Rest/binary>> ->
                {F, O, Payload, Rest};

            <<_:1, _:3, _:4, 1:1, _/binary>> ->
                %% According o rfc6455 5.1 the server must not mask any frames.
                die(Socket, Transport, PPid, {1006, "Protocol error"}, normal);
            _ ->
                moredata
        end,
    case R of
        moredata ->
            State;
        _ -> do_recv2(State, R)
    end.

do_recv2(State = #state{phase = Phase, socket = Socket, ppid = PPid, transport = Transport}, R) ->
    case R of
        {1, 1, Payload, Rest} ->
            PPid ! {rfc6455, recv, self(), Payload},
            State#state{data = Rest};
        {1, 2, Payload, Rest} ->
            PPid ! {rfc6455, recv_binary, self(), Payload},
            State#state{data = Rest};
        {1, 8, Payload, _Rest} ->
            WsReason = case Payload of
                           <<WC:16, WR/binary>> -> {WC, WR};
                           <<>> -> {1005, "No status received"}
                       end,
            case Phase of
                open -> %% echo
                    do_close(State, WsReason),
                    Transport:close(Socket);
                closing ->
                    ok
            end,
            die(Socket, Transport, PPid, WsReason, normal);
        {_, _, _, _Rest} ->
            io:format("Unknown frame type~n"),
            die(Socket, Transport, PPid, {1006, "Unknown frame type"}, normal)
    end.

encode_frame(F, O, Payload) ->
    Mask = crypto:strong_rand_bytes(4),
    MaskedPayload = apply_mask(Mask, iolist_to_binary(Payload)),

    L = byte_size(MaskedPayload),
    IoData = case L of
                 _ when L < 126 ->
                     [<<F:1, 0:3, O:4, 1:1, L:7>>, Mask, MaskedPayload];
                 _ when L < 65536 ->
                     [<<F:1, 0:3, O:4, 1:1, 126:7, L:16>>, Mask, MaskedPayload];
                 _ ->
                     [<<F:1, 0:3, O:4, 1:1, 127:7, L:64>>, Mask, MaskedPayload]
           end,
    iolist_to_binary(IoData).

do_send(State = #state{socket = Socket, transport = Transport}, Payload) ->
    Transport:send(Socket, encode_frame(1, 1, Payload)),
    State.

do_close(State = #state{socket = Socket, transport = Transport}, {Code, Reason}) ->
    Payload = iolist_to_binary([<<Code:16>>, Reason]),
    Transport:send(Socket, encode_frame(1, 8, Payload)),
    State#state{phase = closing}.


loop(State = #state{socket = Socket, transport = Transport, ppid = PPid, data = Data,
                    phase = Phase}) ->
    receive
        {In, Socket, Bin} when In =:= tcp; In =:= ssl ->
            State1 = State#state{data = iolist_to_binary([Data, Bin])},
            loop(do_recv(State1));
        {send, Payload} when Phase == open ->
            loop(do_send(State, Payload));
        {Closed, Socket} when Closed =:= tcp_closed; Closed =:= ssl_closed ->
            die(Socket, Transport, PPid, {1006, "Connection closed abnormally"}, normal);
        {close, WsReason} when Phase == open ->
            loop(do_close(State, WsReason));
        {Error, Socket, Reason} when Error =:= tcp_error; Error =:= ssl_error ->
            die(Socket, Transport, PPid, {1006, "Connection closed abnormally"}, Reason);
        Other ->
            error({unknown_message, Other, Socket})
    end.


die(Socket, Transport, PPid, WsReason, Reason) ->
    Transport:shutdown(Socket, read_write),
    PPid ! {rfc6455, close, self(), WsReason},
    exit(Reason).


%% --------------------------------------------------------------------------

split(SubStr, Str, Limit) ->
    split(SubStr, Str, Limit, "").

split(SubStr, Str, Limit, Default) ->
    Acc = split(SubStr, Str, Limit, [], Default),
    lists:reverse(Acc).
split(_SubStr, Str, 0, Acc, _Default) -> [Str | Acc];
split(SubStr, Str, Limit, Acc, Default) ->
    {L, R} = case string:str(Str, SubStr) of
                 0 -> {Str, Default};
                 I -> {string:substr(Str, 1, I-1),
                       string:substr(Str, I+length(SubStr))}
             end,
    split(SubStr, R, Limit-1, [L | Acc], Default).


apply_mask(Mask, Data) when is_number(Mask) ->
    apply_mask(<<Mask:32>>, Data);

apply_mask(<<0:32>>, Data) ->
    Data;
apply_mask(Mask, Data) ->
    iolist_to_binary(lists:reverse(apply_mask2(Mask, Data, []))).

apply_mask2(M = <<Mask:32>>, <<Data:32, Rest/binary>>, Acc) ->
    T = Data bxor Mask,
    apply_mask2(M, Rest, [<<T:32>> | Acc]);
apply_mask2(<<Mask:24, _:8>>, <<Data:24>>, Acc) ->
    T = Data bxor Mask,
    [<<T:24>> | Acc];
apply_mask2(<<Mask:16, _:16>>, <<Data:16>>, Acc) ->
    T = Data bxor Mask,
    [<<T:16>> | Acc];
apply_mask2(<<Mask:8, _:24>>, <<Data:8>>, Acc) ->
    T = Data bxor Mask,
    [<<T:8>> | Acc];
apply_mask2(_, <<>>, Acc) ->
    Acc.
