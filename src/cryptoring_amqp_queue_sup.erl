%%%-------------------------------------------------------------------
%%% @author ins
%%% @copyright (C) 2020, ins
%%% @doc
%%%
%%% @end
%%% Created : 2020-01-28 09:34:36.584992
%%%-------------------------------------------------------------------
-module(cryptoring_amqp_queue_sup).

-behaviour(supervisor).

%% API
-export([
         start_link/0,
         add_queue_subscription/2
        ]).

%% Supervisor callbacks
-export([init/1]).

-define(SERVER, ?MODULE).

%%%===================================================================
%%% API functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the supervisor
%%
%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

add_queue_subscription(Exchange, RoutingKey) ->
    supervisor:start_child(?SERVER, [self(), Exchange, RoutingKey]).

%%%===================================================================
%%% Supervisor callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Whenever a supervisor is started using supervisor:start_link/[2,3],
%% this function is called by the new process to find out about
%% restart strategy, maximum restart frequency and child
%% specifications.
%%
%% @spec init(Args) -> {ok, {SupFlags, [ChildSpec]}} |
%%                     ignore |
%%                     {error, Reason}
%% @end
%%--------------------------------------------------------------------
init([]) ->
    RestartStrategy = simple_one_for_one,
    MaxRestarts = 1000,
    MaxSecondsBetweenRestarts = 3600,

    SupFlags = {RestartStrategy, MaxRestarts, MaxSecondsBetweenRestarts},

    Restart = permanent,
    Shutdown = 2000,
    Type = worker,

    AChild = {'cryptoring_amqp_queue_srv', {'cryptoring_amqp_queue_srv', start_link, []},
              Restart, Shutdown, Type, ['cryptoring_amqp_queue_srv']},

    {ok, {SupFlags, [AChild]}}.

%%%===================================================================
%%% Internal functions
%%%===================================================================



