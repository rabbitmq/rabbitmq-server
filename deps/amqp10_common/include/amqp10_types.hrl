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
