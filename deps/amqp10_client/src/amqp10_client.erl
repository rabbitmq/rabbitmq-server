-module(amqp10_client).

-include("amqp10_client.hrl").
-include_lib("amqp10_common/include/amqp10_framing.hrl").

-export([
         open_connection/1,
         open_connection/2,
         close_connection/1,
         begin_session/1,
         begin_session_sync/1,
         begin_session_sync/2,
         end_session/1,
         attach_sender_link/3,
         attach_sender_link/4,
         attach_sender_link_sync/3,
         attach_sender_link_sync/4,
         attach_receiver_link/3,
         attach_receiver_link/4,
         detach_link/1,
         send_msg/2,
         accept_msg/2,
         flow_link_credit/3,
         link_handle/1,
         get_msg/1,
         get_msg/2
        ]).

-define(DEFAULT_TIMEOUT, 5000).

-opaque link_ref() :: #link_ref{}.

-export_type([link_ref/0,
              result/2
             ]).

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
-spec open_connection(amqp10_client_connection:connection_config()) ->
    supervisor:startchild_ret().
open_connection(ConnectionConfig0) ->
    Notify = maps:get(notify, ConnectionConfig0, self()),
    amqp10_client_connection:open(ConnectionConfig0#{notify => Notify}).

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
    gen_fsm:send_event(Pid, 'end').

%% @doc Synchronously attach a link on 'Session'.
%% This is a convenience function that awaits attached event
%% for the link before returning.
attach_sender_link_sync(Session, Name, Target) ->
    attach_sender_link_sync(Session, Name, Target, mixed).

%% @doc Synchronously attach a link on 'Session'.
%% This is a convenience function that awaits attached event
%% for the link before returning.
-spec attach_sender_link_sync(pid(), binary(), binary(),
                              amqp10_client_session:snd_settle_mode()) ->
    {ok, link_ref()} | link_timeout.
attach_sender_link_sync(Session, Name, Target, SettleMode) ->
    {ok, Ref} = attach_sender_link(Session, Name, Target, SettleMode),
    receive
        {amqp10_event, {link, {sender, Name}, attached}} ->
            {ok, Ref};
        {amqp10_event, {link, {sender, Name}, {detached, Err}}} ->
            {error, Err}
    after ?DEFAULT_TIMEOUT -> link_timeout
    end.

%% @doc Attaches a sender link to a target.
%% This is asynchronous and will notify completion of the attach request to the
%% caller using an amqp10_event of the following format:
%% {amqp10_event, {link, {sender, Name}, attached | {detached, Why}}}
-spec attach_sender_link(pid(), binary(), binary()) -> {ok, link_ref()}.
attach_sender_link(Session, Name, Target) ->
    % mixed should work with any type of msg
    attach_sender_link(Session, Name, Target, mixed).

%% @doc Attaches a sender link to a target.
%% This is asynchronous and will notify completion of the attach request to the
%% caller using an amqp10_event of the following format:
%% {amqp10_event, {link, {sender, Name}, attached | {detached, Why}}}
-spec attach_sender_link(pid(), binary(), binary(),
                         amqp10_client_session:snd_settle_mode()) ->
    {ok, link_ref()}.
attach_sender_link(Session, Name, Target, SettleMode) ->
    AttachArgs = #{name => Name,
                   role => {sender, #{address => Target}},
                   snd_settle_mode => SettleMode,
                   rcv_settle_mode => first},
    {ok, Attach} = amqp10_client_session:attach(Session, AttachArgs),
    {ok, make_link_ref(sender, Session, Attach)}.

%% @doc Attaches a receiver link to a source.
%% This is asynchronous and will notify completion of the attach request to the
%% caller using an amqp10_event of the following format:
%% {amqp10_event, {link, {receiver, Name}, attached | {detached, Why}}}
-spec attach_receiver_link(pid(), binary(), binary()) ->
    {ok, link_ref()}.
attach_receiver_link(Session, Name, Source) ->
    attach_receiver_link(Session, Name, Source, settled).

%% @doc Attaches a receiver link to a source.
%% This is asynchronous and will notify completion of the attach request to the
%% caller using an amqp10_event of the following format:
%% {amqp10_event, {link, {receiver, Name}, attached | {detached, Why}}}
-spec attach_receiver_link(pid(), binary(), binary(),
                           amqp10_client_session:snd_settle_mode()) ->
    {ok, link_ref()}.
attach_receiver_link(Session, Name, Source, SettleMode) ->
    AttachArgs = #{name => Name,
                   role => {receiver, #{address => Source}, self()},
                   snd_settle_mode => SettleMode,
                   rcv_settle_mode => first},
    {ok, Attach} = amqp10_client_session:attach(Session, AttachArgs),
    {ok, make_link_ref(receiver, Session, Attach)}.

%% @doc Detaches a link.
%% This is asynchronous and will notify completion of the attach request to the
%% caller using an amqp10_event of the following format:
%% {amqp10_event, {link, {sender | receiver, Name}, {detached, Why}}}
-spec detach_link(link_ref()) -> _.
detach_link(#link_ref{link_handle = Handle, session = Session}) ->
    amqp10_client_session:detach(Session, Handle).

%% @doc Grant credit to a sender.
%% The amqp10_client will automatically grant more credit to the sender when
%% the remaining link credit falls below the value of RenewWhenBelow.
%% If RenewWhenBelow is 'never' the client will never grant new credit. Instead
%% the caller will be notified when the link_credit reaches 0 with an
%% amqp10_event of the following format:
%% {amqp10_event, {link, {receiver, Name}, credit_exhausted}}
-spec flow_link_credit(link_ref(), non_neg_integer(), never | non_neg_integer()) -> ok.
flow_link_credit(#link_ref{role = receiver, session = Session,
                           link_handle = Handle}, Credit, RenewWhenBelow) ->
    Flow = #'v1_0.flow'{link_credit = {uint, Credit}},
    ok = amqp10_client_session:flow(Session, Handle, Flow, RenewWhenBelow).


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
get_msg(#link_ref{role = receiver, link_handle = Handle} = Ref,
        Timeout) ->
    %flow 1
    ok = flow_link_credit(Ref, 1, never),
    % wait for transfer
    receive
        {amqp10_msg, Handle, Message} -> {ok, Message}
    after Timeout ->
              {error, timeout}
    end.

%% @doc Get the link handle from a LinkRef
-spec link_handle(link_ref()) -> non_neg_integer().
link_handle(#link_ref{link_handle = Handle}) -> Handle.


%%% Helpers
-spec make_link_ref(_, _, _) -> link_ref().
make_link_ref(Role, Session, Handle) ->
    #link_ref{role = Role, session = Session, link_handle = Handle}.
