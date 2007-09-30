-module(rabbit_direct_channel).

-include_lib("rabbit/include/rabbit.hrl").
-include_lib("rabbit/include/rabbit_framing.hrl").

-export([start/3]).

-define(CLOSING_TIMEOUT, 10).

start(ChannelPid, ChannelNumber, Connection) ->
    {ok, _, _} = read_method(), %% 'channel.open'
    rabbit_writer:send_command(ChannelPid, #'channel.open_ok'{}),
    Tx0 = rabbit_transaction:start(),
    Tx1 = rabbit_transaction:set_writer_pid(Tx0, ChannelPid),
    mainloop(#ch{ channel = ChannelNumber,
                  tx = Tx1,
                  reader_pid = ChannelPid,
                  writer_pid = ChannelPid,
                  username = (Connection#connection.user)#user.username,
                  virtual_host = Connection#connection.vhost,
                  most_recently_declared_queue = <<>>,
                  consumer_mapping = dict:new(),
                  next_ticket = 101 }).

read_method() ->
    receive
        {Sender, Method} ->
            {ok, Method, <<>>};
        {Sender, Method, Content} ->
            {ok, Method, Content}
    end.

%---------------------------------------------------------------------------
% Matthias I copied this almost 1:1 from the framing channel.....if you think
% this module has any merit, then this should get abstracted out to a common module
%---------------------------------------------------------------------------

mainloop(State = #ch{channel = Channel}) ->
    {ok, MethodRecord, Content} = read_method(),
    case rabbit_channel:safe_handle_method(MethodRecord, Content, State) of
        stop -> ok;
        terminate ->
            erlang:send_after(?CLOSING_TIMEOUT * 1000, self(),
                              {force_termination, Channel}),
            termination_loop(Channel);
        NewState -> mainloop(NewState)
    end.

termination_loop(Channel) ->
    case read_method() of
        {method, 'channel.close_ok', _} ->
            ok;
        Frame ->
            rabbit_log:warning("ignoring frame ~p on closed channel ~p~n",
                               [Frame, Channel]),
            termination_loop(Channel)
    end.
