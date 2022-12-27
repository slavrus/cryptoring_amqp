%%%-------------------------------------------------------------------
%% @doc cryptoring_amqp top level supervisor.
%% @end
%%%-------------------------------------------------------------------

-module(cryptoring_amqp_sup).

-behaviour(supervisor).

%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

-define(SERVER, ?MODULE).

%%====================================================================
%% API functions
%%====================================================================

start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

%%====================================================================
%% Supervisor callbacks
%%====================================================================

%% Child :: {Id,StartFunc,Restart,Shutdown,Type,Modules}
init([]) ->
    Children = [
                #{id => cryptoring_amqp_queue_sup,
                  start => {cryptoring_amqp_queue_sup, start_link, []},
                  type => supervisor},
                #{id => cryptoring_amqp_connection,
                  start => {cryptoring_amqp_connection, start_link, []}},
                #{id => cryptoring_amqp_log,
                  start => {cryptoring_amqp_log, start_link, []}}
               ],
    {ok, { {one_for_one, 10, 1}, Children} }.

%%====================================================================
%% Internal functions
%%====================================================================
