%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2007-2021 VMware, Inc. or its affiliates.  All rights reserved.
%%

-module(amqp10_client).

-include("amqp10_client.hrl").
-include_lib("amqp10_common/include/amqp10_framing.hrl").

-export([open_connection/1,
         open_connection/2,
         close_connection/1,
         begin_session/1,
         begin_session_sync/1,
         begin_session_sync/2,
         end_session/1,
         attach_sender_link/3,
         attach_sender_link/4,
         attach_sender_link/5,
         attach_sender_link_sync/3,
         attach_sender_link_sync/4,
         attach_sender_link_sync/5,
         attach_receiver_link/3,
         attach_receiver_link/4,
         attach_receiver_link/5,
         attach_receiver_link/6,
         attach_receiver_link/7,
         attach_link/2,
         detach_link/1,
         send_msg/2,
         accept_msg/2,
         flow_link_credit/3,
         flow_link_credit/4,
         echo/1,
         link_handle/1,
         get_msg/1,
         get_msg/2,
         parse_uri/1
        ]).

-define(DEFAULT_TIMEOUT, 5000).

-type snd_settle_mode() :: amqp10_client_session:snd_settle_mode().
-type rcv_settle_mode() :: amqp10_client_session:rcv_settle_mode().

-type terminus_durability() :: amqp10_client_session:terminus_durability().

-type target_def() :: amqp10_client_session:target_def().
-type source_def() :: amqp10_client_session:source_def().

-type attach_role() :: amqp10_client_session:attach_role().
-type attach_args() :: amqp10_client_session:attach_args().
-type filter() :: amqp10_client_session:filter().
-type properties() :: amqp10_client_session:properties().

-type connection_config() :: amqp10_client_connection:connection_config().

-opaque link_ref() :: #link_ref{}.

-export_type([
              link_ref/0,
              snd_settle_mode/0,
              rcv_settle_mode/0,
              terminus_durability/0,
              target_def/0,
              source_def/0,
              attach_role/0,
              attach_args/0,
              connection_config/0
             ]).

-ifdef (OTP_RELEASE).
  -if(?OTP_RELEASE >= 23).
    -compile({nowarn_deprecated_function, [{http_uri, decode, 1}]}).
  -endif.
-endif.

%% @doc Convenience function for opening a connection providing only an
%% address and port. This uses anonymous sasl authentication.
%% This is asynchronous and will notify success/closure to the caller using
%% an amqp10_event of the following format:
%% {amqp10_event, {connection, ConnectionPid, opened | {closed, Why}}}
-spec open_connection(inet:socket_address() | inet:hostname(),
                      inet:port_number()) -> supervisor:startchild_ret().
open_connection(Addr, Port) ->
    open_connection(#{address => Addr, port => Port, notify => self(),
                      sasl => anon}).

%% @doc Opens a connection using a connection_config map
%% This is asynchronous and will notify success/closure to the caller using
%% an amqp10_event of the following format:
%% {amqp10_event, {connection, ConnectionPid, opened | {closed, Why}}}
-spec open_connection(connection_config()) ->
    supervisor:startchild_ret().
open_connection(ConnectionConfig0) ->
    Notify = maps:get(notify, ConnectionConfig0, self()),
    NotifyWhenOpened = maps:get(notify_when_opened, ConnectionConfig0, self()),
    NotifyWhenClosed = maps:get(notify_when_closed, ConnectionConfig0, self()),
    amqp10_client_connection:open(ConnectionConfig0#{
        notify => Notify,
        notify_when_opened => NotifyWhenOpened,
        notify_when_closed => NotifyWhenClosed
    }).

%% @doc Opens a connection using a connection_config map
%% This is asynchronous and will notify completion to the caller using
%% an amqp10_event of the following format:
%% {amqp10_event, {connection, ConnectionPid, {closed, Why}}}
-spec close_connection(pid()) -> ok.
close_connection(Pid) ->
    amqp10_client_connection:close(Pid, none).

%% @doc Begins an amqp10 session using 'Connection'.
%% This is asynchronous and will notify success/closure to the caller using
%% an amqp10_event of the following format:
%% {amqp10_event, {session, SessionPid, begun | {ended, Why}}}
-spec begin_session(pid()) -> supervisor:startchild_ret().
begin_session(Connection) when is_pid(Connection) ->
    amqp10_client_connection:begin_session(Connection).

%% @doc Synchronously begins an amqp10 session using 'Connection'.
%% This is a convenience function that awaits the 'begun' event
%% for the newly created session before returning.
-spec begin_session_sync(pid()) ->
    supervisor:startchild_ret() | session_timeout.
begin_session_sync(Connection) when is_pid(Connection) ->
    begin_session_sync(Connection, ?DEFAULT_TIMEOUT).

%% @doc Synchronously begins an amqp10 session using 'Connection'.
%% This is a convenience function that awaits the 'begun' event
%% for the newly created session before returning.
-spec begin_session_sync(pid(), non_neg_integer()) ->
    supervisor:startchild_ret() | session_timeout.
begin_session_sync(Connection, Timeout) when is_pid(Connection) ->
    case begin_session(Connection) of
        {ok, Session} ->
            receive
                {amqp10_event, {session, Session, begun}} ->
                    {ok, Session};
                {amqp10_event, {session, Session, {ended, Err}}} ->
                    {error, Err}
            after Timeout -> session_timeout
            end;
        Ret -> Ret
    end.

%% @doc End an amqp10 session.
%% This is asynchronous and will notify completion of the end request to the
%% caller using an amqp10_event of the following format:
%% {amqp10_event, {session, SessionPid, {ended, Why}}}
-spec end_session(pid()) -> ok.
end_session(Pid) ->
    amqp10_client_session:'end'(Pid).

%% @doc Synchronously attach a link on 'Session'.
%% This is a convenience function that awaits attached event
%% for the link before returning.
attach_sender_link_sync(Session, Name, Target) ->
    attach_sender_link_sync(Session, Name, Target, mixed).

%% @doc Synchronously attach a link on 'Session'.
%% This is a convenience function that awaits attached event
%% for the link before returning.
-spec attach_sender_link_sync(pid(), binary(), binary(),
                              snd_settle_mode()) ->
    {ok, link_ref()} | link_timeout.
attach_sender_link_sync(Session, Name, Target, SettleMode) ->
    attach_sender_link_sync(Session, Name, Target, SettleMode, none).

%% @doc Synchronously attach a link on 'Session'.
%% This is a convenience function that awaits attached event
%% for the link before returning.
-spec attach_sender_link_sync(pid(), binary(), binary(),
                              snd_settle_mode(), terminus_durability()) ->
    {ok, link_ref()} | link_timeout.
attach_sender_link_sync(Session, Name, Target, SettleMode, Durability) ->
    {ok, Ref} = attach_sender_link(Session, Name, Target, SettleMode,
                                   Durability),
    receive
        {amqp10_event, {link, Ref, attached}} ->
            {ok, Ref};
        {amqp10_event, {link, Ref, {detached, Err}}} ->
            {error, Err}
    after ?DEFAULT_TIMEOUT -> link_timeout
    end.

%% @doc Attaches a sender link to a target.
%% This is asynchronous and will notify completion of the attach request to the
%% caller using an amqp10_event of the following format:
%% {amqp10_event, {link, LinkRef, attached | {detached, Why}}}
-spec attach_sender_link(pid(), binary(), binary()) -> {ok, link_ref()}.
attach_sender_link(Session, Name, Target) ->
    % mixed should work with any type of msg
    attach_sender_link(Session, Name, Target, mixed).

%% @doc Attaches a sender link to a target.
%% This is asynchronous and will notify completion of the attach request to the
%% caller using an amqp10_event of the following format:
%% {amqp10_event, {link, LinkRef, attached | {detached, Why}}}
-spec attach_sender_link(pid(), binary(), binary(),
                         snd_settle_mode()) ->
    {ok, link_ref()}.
attach_sender_link(Session, Name, Target, SettleMode) ->
    attach_sender_link(Session, Name, Target, SettleMode, none).

%% @doc Attaches a sender link to a target.
%% This is asynchronous and will notify completion of the attach request to the
%% caller using an amqp10_event of the following format:
%% {amqp10_event, {link, LinkRef, attached | {detached, Why}}}
-spec attach_sender_link(pid(), binary(), binary(),
                         snd_settle_mode(), terminus_durability()) ->
    {ok, link_ref()}.
attach_sender_link(Session, Name, Target, SettleMode, Durability) ->
    AttachArgs = #{name => Name,
                   role => {sender, #{address => Target,
                                      durable => Durability}},
                   snd_settle_mode => SettleMode,
                   rcv_settle_mode => first},
    amqp10_client_session:attach(Session, AttachArgs).

%% @doc Attaches a receiver link to a source.
%% This is asynchronous and will notify completion of the attach request to the
%% caller using an amqp10_event of the following format:
%% {amqp10_event, {link, LinkRef, attached | {detached, Why}}}
-spec attach_receiver_link(pid(), binary(), binary()) ->
    {ok, link_ref()}.
attach_receiver_link(Session, Name, Source) ->
    attach_receiver_link(Session, Name, Source, settled).

%% @doc Attaches a receiver link to a source.
%% This is asynchronous and will notify completion of the attach request to the
%% caller using an amqp10_event of the following format:
%% {amqp10_event, {link, LinkRef, attached | {detached, Why}}}
-spec attach_receiver_link(pid(), binary(), binary(),
                           snd_settle_mode()) ->
    {ok, link_ref()}.
attach_receiver_link(Session, Name, Source, SettleMode) ->
    attach_receiver_link(Session, Name, Source, SettleMode, none).

%% @doc Attaches a receiver link to a source.
%% This is asynchronous and will notify completion of the attach request to the
%% caller using an amqp10_event of the following format:
%% {amqp10_event, {link, LinkRef, attached | {detached, Why}}}
-spec attach_receiver_link(pid(), binary(), binary(),
                           snd_settle_mode(), terminus_durability()) ->
    {ok, link_ref()}.
attach_receiver_link(Session, Name, Source, SettleMode, Durability) ->
    attach_receiver_link(Session, Name, Source, SettleMode, Durability, #{}).

%% @doc Attaches a receiver link to a source.
%% This is asynchronous and will notify completion of the attach request to the
%% caller using an amqp10_event of the following format:
%% {amqp10_event, {link, LinkRef, attached | {detached, Why}}}
-spec attach_receiver_link(pid(), binary(), binary(),
                           snd_settle_mode(), terminus_durability(), filter()) ->
    {ok, link_ref()}.
attach_receiver_link(Session, Name, Source, SettleMode, Durability, Filter) ->
    attach_receiver_link(Session, Name, Source, SettleMode, Durability, Filter, #{}).

%% @doc Attaches a receiver link to a source.
%% This is asynchronous and will notify completion of the attach request to the
%% caller using an amqp10_event of the following format:
%% {amqp10_event, {link, LinkRef, attached | {detached, Why}}}
-spec attach_receiver_link(pid(), binary(), binary(),
                           snd_settle_mode(), terminus_durability(), filter(),
                           properties()) ->
    {ok, link_ref()}.
attach_receiver_link(Session, Name, Source, SettleMode, Durability, Filter, Properties) ->
    AttachArgs = #{name => Name,
                   role => {receiver, #{address => Source,
                                        durable => Durability}, self()},
                   snd_settle_mode => SettleMode,
                   rcv_settle_mode => first,
                   filter => Filter,
                   properties => Properties},
    amqp10_client_session:attach(Session, AttachArgs).

-spec attach_link(pid(), attach_args()) -> {ok, link_ref()}.
attach_link(Session, AttachArgs) ->
    amqp10_client_session:attach(Session, AttachArgs).

%% @doc Detaches a link.
%% This is asynchronous and will notify completion of the attach request to the
%% caller using an amqp10_event of the following format:
%% {amqp10_event, {link, LinkRef, {detached, Why}}}
-spec detach_link(link_ref()) -> _.
detach_link(#link_ref{link_handle = Handle, session = Session}) ->
    amqp10_client_session:detach(Session, Handle).

%% @doc Grant credit to a sender.
%% The amqp10_client will automatically grant more credit to the sender when
%% the remaining link credit falls below the value of RenewWhenBelow.
%% If RenewWhenBelow is 'never' the client will never grant new credit. Instead
%% the caller will be notified when the link_credit reaches 0 with an
%% amqp10_event of the following format:
%% {amqp10_event, {link, LinkRef, credit_exhausted}}
-spec flow_link_credit(link_ref(), Credit :: non_neg_integer(),
                       RenewWhenBelow :: never | non_neg_integer()) -> ok.
flow_link_credit(Ref, Credit, RenewWhenBelow) ->
    flow_link_credit(Ref, Credit, RenewWhenBelow, false).

-spec flow_link_credit(link_ref(), Credit :: non_neg_integer(),
                       RenewWhenBelow :: never | non_neg_integer(),
                       Drain :: boolean()) -> ok.
flow_link_credit(#link_ref{role = receiver, session = Session,
                           link_handle = Handle},
                 Credit, RenewWhenBelow, Drain) ->
    Flow = #'v1_0.flow'{link_credit = {uint, Credit},
                        drain = Drain},
    ok = amqp10_client_session:flow(Session, Handle, Flow, RenewWhenBelow).

%% @doc Request that the sender's flow state is echoed back
%% This may be used to determine when the Link has finally quiesced.
%% see §2.6.10 of the spec
echo(#link_ref{role = receiver, session = Session,
               link_handle = Handle}) ->
    Flow = #'v1_0.flow'{link_credit = {uint, 0},
                        echo = true},
    ok = amqp10_client_session:flow(Session, Handle, Flow, 0).

%%% messages

%% @doc Send a message on a the link referred to be the 'LinkRef'.
%% Returns ok for "async" transfers when messages are sent with settled=true
%% else it returns the delivery state from the disposition
-spec send_msg(link_ref(), amqp10_msg:amqp10_msg()) ->
    ok | {error, insufficient_credit | link_not_found | half_attached}.
send_msg(#link_ref{role = sender, session = Session,
                   link_handle = Handle}, Msg0) ->
    Msg = amqp10_msg:set_handle(Handle, Msg0),
    amqp10_client_session:transfer(Session, Msg, ?DEFAULT_TIMEOUT).

%% @doc Accept a message on a the link referred to be the 'LinkRef'.
-spec accept_msg(link_ref(), amqp10_msg:amqp10_msg()) -> ok.
accept_msg(#link_ref{role = receiver, session = Session}, Msg) ->
    DeliveryId = amqp10_msg:delivery_id(Msg),
    amqp10_client_session:disposition(Session, receiver, DeliveryId,
                                      DeliveryId, true, accepted).

%% @doc Get a single message from a link.
%% Flows a single link credit then awaits delivery or timeout.
-spec get_msg(link_ref()) -> {ok, amqp10_msg:amqp10_msg()} | {error, timeout}.
get_msg(LinkRef) ->
    get_msg(LinkRef, ?DEFAULT_TIMEOUT).

%% @doc Get a single message from a link.
%% Flows a single link credit then awaits delivery or timeout.
-spec get_msg(link_ref(), non_neg_integer()) ->
    {ok, amqp10_msg:amqp10_msg()} | {error, timeout}.
get_msg(#link_ref{role = receiver} = Ref, Timeout) ->
    %flow 1
    ok = flow_link_credit(Ref, 1, never),
    % wait for transfer
    receive
        {amqp10_msg, Ref, Message} -> {ok, Message}
    after Timeout ->
              {error, timeout}
    end.

%% @doc Get the link handle from a LinkRef
-spec link_handle(link_ref()) -> non_neg_integer().
link_handle(#link_ref{link_handle = Handle}) -> Handle.


-spec parse_uri(string()) ->
    {ok, connection_config()} | {error, term()}.
parse_uri(Uri) ->
    case uri_string:parse(Uri) of
        Map when is_map(Map) ->
            try
                {ok, parse_result(Map)}
            catch
                throw:Err -> {error, Err}
            end;
        {error, _, _} = Err -> Err
    end.

parse_result(Map) ->
    _ = case maps:get(path, Map, "/") of
      "/" -> ok;
      ""  -> ok;
      _   -> throw(path_segment_not_supported)
    end,
    Scheme   = maps:get(scheme, Map, "amqp"),
    UserInfo = maps:get(userinfo, Map, undefined),
    Host     = maps:get(host, Map),
    DefaultPort = case Scheme of
      "amqp"  -> 5672;
      "amqps" -> 5671
    end,
    Port   = maps:get(port, Map, DefaultPort),
    Query0 = maps:get(query, Map, ""),
    Query  = maps:from_list(uri_string:dissect_query(Query0)),
    Sasl = case Query of
               #{"sasl" := "anon"} -> anon;
               #{"sasl" := "plain"} when UserInfo =:= undefined orelse length(UserInfo) =:= 0 ->
                   throw(plain_sasl_missing_userinfo);
               _ ->
                   case UserInfo of
                       [] -> none;
                       undefined -> none;
                       U -> parse_usertoken(U)
                   end
           end,
    Ret0 = maps:fold(fun("idle_time_out", V, Acc) ->
                             Acc#{idle_time_out => list_to_integer(V)};
                        ("max_frame_size", V, Acc) ->
                             Acc#{max_frame_size => list_to_integer(V)};
                        ("hostname", V, Acc) ->
                             Acc#{hostname => list_to_binary(V)};
                        ("container_id", V, Acc) ->
                             Acc#{container_id => list_to_binary(V)};
                        ("transfer_limit_margin", V, Acc) ->
                             Acc#{transfer_limit_margin => list_to_integer(V)};
                        (_, _, Acc) -> Acc
                     end, #{address => Host,
                            hostname => to_binary(Host),
                            port => Port,
                            sasl => Sasl}, Query),
    case Scheme of
        "amqp"  -> Ret0;
        "amqps" ->
            TlsOpts = parse_tls_opts(Query),
            Ret0#{tls_opts => {secure_port, TlsOpts}}
    end.


parse_usertoken(undefined) ->
    none;
parse_usertoken("") ->
    none;
parse_usertoken(U) ->
    [User, Pass] = string:tokens(U, ":"),
    {plain,
     to_binary(uri_string:percent_decode(User)),
     to_binary(uri_string:percent_decode(Pass))}.

parse_tls_opts(M) ->
    lists:sort(maps:fold(fun parse_tls_opt/3, [], M)).

parse_tls_opt(K, V, Acc)
  when K =:= "cacertfile" orelse
       K =:= "certfile" orelse
       K =:= "keyfile" ->
    [{to_atom(K), V} | Acc];
parse_tls_opt("verify", V, Acc)
  when V =:= "verify_none" orelse
       V =:= "verify_peer" ->
    [{verify, to_atom(V)} | Acc];
parse_tls_opt("verify", _V, _Acc) ->
    throw({invalid_option, verify});
parse_tls_opt("versions", V, Acc) ->
    Parts = string:tokens(V, ","),
    Versions = [try_to_existing_atom(P) || P <- Parts],
    [{versions, Versions} | Acc];
parse_tls_opt("fail_if_no_peer_cert", V, Acc)
  when V =:= "true" orelse
       V =:= "false" ->
    [{fail_if_no_peer_cert, to_atom(V)} | Acc];
parse_tls_opt("fail_if_no_peer_cert", _V, _Acc) ->
    throw({invalid_option, fail_if_no_peer_cert});
parse_tls_opt("server_name_indication", "disable", Acc) ->
    [{server_name_indication, disable} | Acc];
parse_tls_opt("server_name_indication", V, Acc) ->
    [{server_name_indication, V} | Acc];
parse_tls_opt(_K, _V, Acc) ->
    Acc.

to_atom(X) when is_list(X) -> list_to_atom(X).
to_binary(X) when is_list(X) -> list_to_binary(X).

try_to_existing_atom(L) when is_list(L) ->
    try list_to_existing_atom(L) of
        A -> A
    catch
        _:badarg ->
            throw({non_existent_atom, L})
    end.


-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

parse_uri_test_() ->
    [?_assertEqual({ok, #{address => "my_host",
                          port => 9876,
                          hostname => <<"my_host">>,
                          sasl => none}}, parse_uri("amqp://my_host:9876")),
     %% port defaults
     ?_assertMatch({ok, #{port := 5671}}, parse_uri("amqps://my_host")),
     ?_assertMatch({ok, #{port := 5672}}, parse_uri("amqp://my_host")),
     ?_assertEqual({ok, #{address => "my_proxy",
                          port => 9876,
                          hostname => <<"my_host">>,
                          container_id => <<"my_container">>,
                          idle_time_out => 60000,
                          max_frame_size => 512,
                          tls_opts => {secure_port, []},
                          sasl => {plain, <<"fred">>, <<"passw">>}}},
                   parse_uri("amqps://fred:passw@my_proxy:9876?sasl=plain&"
                             "hostname=my_host&container_id=my_container&"
                             "max_frame_size=512&idle_time_out=60000")),
     %% ensure URI encoded usernames and passwords are decodeded
     ?_assertEqual({ok, #{address => "my_proxy",
                          port => 9876,
                          hostname => <<"my_proxy">>,
                          sasl => {plain, <<"fr/ed">>, <<"pa/ssw">>}}},
                   parse_uri("amqp://fr%2Fed:pa%2Fssw@my_proxy:9876")),
     %% make sasl plain implicit when username and password is present
     ?_assertEqual({ok, #{address => "my_proxy",
                          port => 9876,
                          hostname => <<"my_proxy">>,
                          sasl => {plain, <<"fred">>, <<"passw">>}}},
                   parse_uri("amqp://fred:passw@my_proxy:9876")),
     ?_assertEqual(
        {ok, #{address => "my_proxy", port => 9876,
               hostname => <<"my_proxy">>,
               tls_opts => {secure_port, [{cacertfile, "/etc/cacertfile.pem"},
                                          {certfile, "/etc/certfile.pem"},
                                          {fail_if_no_peer_cert, true},
                                          {keyfile, "/etc/keyfile.key"},
                                          {server_name_indication, "frazzle"},
                                          {verify, verify_none},
                                          {versions, ['tlsv1.1', 'tlsv1.2']}
                                         ]},
               sasl => {plain, <<"fred">>, <<"passw">>}}},
        parse_uri("amqps://fred:passw@my_proxy:9876?sasl=plain"
                  "&cacertfile=/etc/cacertfile.pem&certfile=/etc/certfile.pem"
                  "&keyfile=/etc/keyfile.key&verify=verify_none"
                  "&fail_if_no_peer_cert=true"
                  "&server_name_indication=frazzle"
                  "&versions=tlsv1.1,tlsv1.2")),
     %% invalid tls version
     ?_assertEqual({error, {non_existent_atom, "tlsv1.9999999"}},
                   parse_uri("amqps://fred:passw@my_proxy:9876?sasl=plain&" ++
                  "versions=tlsv1.1,tlsv1.9999999")),
     ?_assertEqual(
        {ok, #{address => "my_proxy",
               port => 9876,
               hostname => <<"my_proxy">>,
               tls_opts => {secure_port, [{server_name_indication, disable}]},
               sasl => {plain, <<"fred">>, <<"passw">>}}},
        parse_uri("amqps://fred:passw@my_proxy:9876?sasl=plain"
                  "&server_name_indication=disable")),
     ?_assertEqual({error, {invalid_option, verify}},
                   parse_uri("amqps://fred:passw@my_proxy:9876?sasl=plain&" ++
                  "cacertfile=/etc/cacertfile.pem&certfile=/etc/certfile.pem&" ++
                  "keyfile=/etc/keyfile.key&verify=verify_bananas")),
     ?_assertEqual({error, {invalid_option, fail_if_no_peer_cert}},
                   parse_uri("amqps://fred:passw@my_proxy:9876?sasl=plain&" ++
                  "cacertfile=/etc/cacertfile.pem&certfile=/etc/certfile.pem&" ++
                  "keyfile=/etc/keyfile.key&fail_if_no_peer_cert=banana")),
     ?_assertEqual({error, plain_sasl_missing_userinfo},
                   parse_uri("amqp://my_host:9876?sasl=plain")),
     ?_assertEqual({error, path_segment_not_supported},
                   parse_uri("amqp://my_host/my_path_segment:9876"))
    ].

-endif.
