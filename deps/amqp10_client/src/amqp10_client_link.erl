-module(amqp10_client_link).

-include("amqp10_client.hrl").
-include_lib("amqp10_common/include/amqp10_framing.hrl").

-export([
         sender/3,
         sender/4,
         receiver/3,
         receiver/4,
         send/2,
         get_one/1,
         get_one/2,
         accept/2
        ]).


-define(DEFAULT_TIMEOUT, 5000).

-record(link_ref, {role :: sender | receiver,session :: pid(),
                   link_handle :: non_neg_integer(), link_name :: binary()}).
-opaque link_ref() :: #link_ref{}.

-export_type([link_ref/0
              ]).

get_one(LinkRef) ->
    get_one(LinkRef, ?DEFAULT_TIMEOUT).

get_one(#link_ref{role = receiver, session = Session, link_handle = Handle},
        Timeout) ->
    %flow 1
    Flow = #'v1_0.flow'{link_credit = {uint, 1}},
    ok = amqp10_client_session:flow(Session, Handle, Flow),
    % wait for transfer
    receive
        {message, Handle, Message} -> {amqp_msg, Message}
    after Timeout ->
              {error, timeout}
    end.


% Returns ok for "async" transfers when messages are send with settled=true
% else it returns the delivery state from the disposition
% TODO: timeouts
-spec send(link_ref(), amqp10_msg:amqp10_msg()) ->
    ok | insufficient_credit | amqp10_client_types:delivery_state().
send(#link_ref{role = sender, session = Session,
               link_handle = Handle}, Msg0) ->
    Msg = amqp10_msg:set_handle(Handle, Msg0),
    amqp10_client_session:transfer(Session, Msg, ?DEFAULT_TIMEOUT).

-spec accept(link_ref(), amqp10_msg:amqp10_msg()) -> ok.
accept(#link_ref{role = receiver, session = Session}, Msg) ->
    DeliveryId = amqp10_msg:delivery_id(Msg),
    amqp10_client_session:disposition(Session, DeliveryId, DeliveryId, true,
                                      accepted).

-spec sender(pid(), binary(), binary()) -> {ok, link_ref()}.
sender(Session, Name, Target) ->
    sender(Session, Name, Target, settled).

-spec sender(pid(), binary(), binary(),
             amqp10_client_session:snd_settle_mode()) -> {ok, link_ref()}.
sender(Session, Name, Target, SettleMode) ->
    AttachArgs = #{name => Name,
                   role => {sender, #{address => Target}},
                   snd_settle_mode => SettleMode,
                   rcv_settle_mode => first},

    {ok, Attach} = amqp10_client_session:attach(Session, AttachArgs),
    {ok, #link_ref{role = sender, session = Session, link_name = Name,
                   link_handle = Attach}}.

-spec receiver(pid(), binary(), binary()) -> {ok, link_ref()}.
receiver(Session, Name, Source) ->
    receiver(Session, Name, Source, settled).

-spec receiver(pid(), binary(), binary(),
               amqp10_client_session:snd_settle_mode()) -> {ok, link_ref()}.
receiver(Session, Name, Source, SettleMode) ->
    AttachArgs = #{name => Name,
                   role => {receiver, #{address => Source}, self()},
                   snd_settle_mode => SettleMode,
                   rcv_settle_mode => first},
    {ok, Attach} = amqp10_client_session:attach(Session, AttachArgs),
    {ok, #link_ref{role = receiver, session = Session, link_name = Name,
                   link_handle = Attach}}.



