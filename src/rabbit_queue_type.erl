-module(rabbit_queue_type).

-export([
         ]).

-optional_callbacks([init/0]).

-callback init() -> #{}.
