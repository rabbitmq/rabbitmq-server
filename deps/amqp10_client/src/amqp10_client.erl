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
         attach_receiver_link/3,
         attach_receiver_link/4,
         send_msg/2,
         accept_msg/2,
         flow_link_credit/2,
         link_handle/1,
         get_msg/1,
         get_msg/2
         % get_msg/1,
         % get_msg/2,
        ]).

-define(DEFAULT_TIMEOUT, 5000).

-record(link_ref, {role :: sender | receiver,session :: pid(),
                   link_handle :: non_neg_integer(), link_name :: binary()}).
-opaque link_ref() :: #link_ref{}.

-export_type([link_ref/0
             ]).
-spec open_connection(
        inet:socket_address() | inet:hostname(),
        inet:port_number()) -> supervisor:startchild_ret().
open_connection(Addr, Port) ->
    open_connection(#{address => Addr, port => Port, notify => self()}).

-spec open_connection(amqp10_client_connection:connection_config()) ->
    supervisor:startchild_ret().
open_connection(ConnectionConfig0) ->
    Notify = maps:get(notify, ConnectionConfig0, self()),
    amqp10_client_connection:open(ConnectionConfig0#{notify => Notify}).

-spec close_connection(pid()) -> ok.
close_connection(Pid) ->
    amqp10_client_connection:close(Pid, none).

-spec begin_session(pid()) -> supervisor:startchild_ret().
begin_session(Connection) when is_pid(Connection) ->
    amqp10_client_connection:begin_session(Connection).

-spec begin_session_sync(pid()) ->
    supervisor:startchild_ret() | session_timeout.
begin_session_sync(Connection) when is_pid(Connection) ->
    begin_session_sync(Connection, ?DEFAULT_TIMEOUT).

-spec begin_session_sync(pid(), non_neg_integer()) ->
    supervisor:startchild_ret() | session_timeout.
begin_session_sync(Connection, Timeout) when is_pid(Connection) ->
    case begin_session(Connection) of
        {ok, Session} ->
            receive
                {amqp10_event, {session, Session, begun}} ->
                    {ok, Session};
                {amqp10_event, {session, Session, {error, Err}}} ->
                    {error, Err}
            after Timeout -> session_timeout
            end;
        Ret -> Ret
    end.

-spec end_session(pid()) -> ok.
end_session(Pid) ->
    gen_fsm:send_event(Pid, 'end').


-spec attach_sender_link(pid(), binary(), binary()) ->
    {ok, amqp10_client_link:link_ref()}.
attach_sender_link(Session, Name, Target) ->
    % mixed should work with any type of msg
    attach_sender_link(Session, Name, Target, mixed).

-spec attach_sender_link(pid(), binary(), binary(),
                         amqp10_client_session:snd_settle_mode()) ->
    {ok, amqp10_client_link:link_ref()}.
attach_sender_link(Session, Name, Target, SettleMode) ->
    AttachArgs = #{name => Name,
                   role => {sender, #{address => Target}},
                   snd_settle_mode => SettleMode,
                   rcv_settle_mode => first},

    {ok, Attach} = amqp10_client_session:attach(Session, AttachArgs),
    {ok, make_link_ref(sender, Session, Name, Attach)}.

-spec attach_receiver_link(pid(), binary(), binary()) ->
    {ok, amqp10_client_link:link_ref()}.
attach_receiver_link(Session, Name, Source) ->
    attach_receiver_link(Session, Name, Source, settled).

-spec attach_receiver_link(pid(), binary(), binary(),
                           amqp10_client_session:snd_settle_mode()) ->
    {ok, amqp10_client_link:link_ref()}.
attach_receiver_link(Session, Name, Source, SettleMode) ->
    AttachArgs = #{name => Name,
                   role => {receiver, #{address => Source}, self()},
                   snd_settle_mode => SettleMode,
                   rcv_settle_mode => first},
    {ok, Attach} = amqp10_client_session:attach(Session, AttachArgs),
    {ok, make_link_ref(receiver, Session, Name, Attach)}.

-spec flow_link_credit(link_ref(), non_neg_integer()) -> ok.
flow_link_credit(#link_ref{role = receiver, session = Session,
                           link_handle = Handle}, Credit) ->
    Flow = #'v1_0.flow'{link_credit = {uint, Credit}},
    ok = amqp10_client_session:flow(Session, Handle, Flow).


%%% messages

% Returns ok for "async" transfers when messages are send with settled=true
% else it returns the delivery state from the disposition
% TODO: timeouts
-spec send_msg(link_ref(), amqp10_msg:amqp10_msg()) ->
    {ok, non_neg_integer()} | {error, insufficient_credit | link_not_found}.
send_msg(#link_ref{role = sender, session = Session,
                   link_handle = Handle}, Msg0) ->
    Msg = amqp10_msg:set_handle(Handle, Msg0),
    amqp10_client_session:transfer(Session, Msg, ?DEFAULT_TIMEOUT).

-spec accept_msg(link_ref(), amqp10_msg:amqp10_msg()) -> ok.
accept_msg(#link_ref{role = receiver, session = Session}, Msg) ->
    DeliveryId = amqp10_msg:delivery_id(Msg),
    amqp10_client_session:disposition(Session, receiver, DeliveryId,
                                      DeliveryId, true, accepted).

get_msg(LinkRef) ->
    get_msg(LinkRef, ?DEFAULT_TIMEOUT).

get_msg(#link_ref{role = receiver, session = Session, link_handle = Handle},
        Timeout) ->
    %flow 1
    Flow = #'v1_0.flow'{link_credit = {uint, 1}},
    ok = amqp10_client_session:flow(Session, Handle, Flow),
    % wait for transfer
    receive
        {amqp10_msg, Handle, Message} -> {ok, Message}
    after Timeout ->
              {error, timeout}
    end.

link_handle(#link_ref{link_handle = Handle}) -> Handle.


%%% Helpers
make_link_ref(Role, Session, Name, Handle) ->
    #link_ref{role = Role, session = Session, link_name = Name,
              link_handle = Handle}.
