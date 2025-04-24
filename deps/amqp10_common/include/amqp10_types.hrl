-define(UINT_MAX, 16#ff_ff_ff_ff).

% [1.6.5]
-type uint() :: 0..?UINT_MAX.
% [2.8.4]
-type link_handle() :: uint().
% [2.8.8]
-type delivery_number() :: sequence_no().
% [2.8.9]
-type transfer_number() :: sequence_no().
% [2.8.10]
-type sequence_no() :: uint().

% [2.8.1]
-define(AMQP_ROLE_SENDER, false).
-define(AMQP_ROLE_RECEIVER, true).

% [2.8.2]
-type snd_settle_mode() :: unsettled | settled | mixed.
% [2.8.3]
-type rcv_settle_mode() :: first | second.

% [3.2.16]
-define(MESSAGE_FORMAT, 0).

%% SQL-based filtering syntax
%% These descriptors are defined in
%% https://www.amqp.org/specification/1.0/filters
-define(DESCRIPTOR_NAME_SELECTOR_FILTER, <<"apache.org:selector-filter:string">>).
-define(DESCRIPTOR_CODE_SELECTOR_FILTER, 16#0000468C00000004).

%% A filter with this name contains a JMS message selector.
%% We use the same name as Qpid JMS in
%% https://github.com/apache/qpid-jms/blob/2.7.0/qpid-jms-client/src/main/java/org/apache/qpid/jms/provider/amqp/AmqpSupport.java#L75
-define(FILTER_NAME_JMS, <<"jms-selector">>).
