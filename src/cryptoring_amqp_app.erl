%%%-------------------------------------------------------------------
%% @doc cryptoring_amqp public API
%% @end
%%%-------------------------------------------------------------------

-module(cryptoring_amqp_app).

-behaviour(application).

%% Application callbacks
-export([start/2, stop/1]).

%%====================================================================
%% API
%%====================================================================

start(_StartType, _StartArgs) ->
    cryptoring_amqp_sup:start_link().

%%--------------------------------------------------------------------
stop(_State) ->
    ok.

%%====================================================================
%% Internal functions
%%====================================================================
