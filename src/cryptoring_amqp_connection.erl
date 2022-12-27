%%%-------------------------------------------------------------------
%%% @author ins
%%% @copyright (C) 2020, ins
%%% @doc
%%%
%%% @end
%%% Created : 2020-01-20 12:43:20.548168
%%%-------------------------------------------------------------------
-module(cryptoring_amqp_connection).

-behaviour(gen_server).

%% API
-export([
         start_link/0,
         get_channel/0,
         declare_exchange/3,
         create_queue/2,
         bind/3,
         bind/4,
         subscribe_to_queue/2,
         subscribe_to_queue/3,
         set_prefetch/2
        ]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-include("cryptoring_amqp.hrl").

-define(SERVER, ?MODULE).

-record(state, {amqp_params :: #amqp_params_network{}
               ,connection :: pid() | undefined
               ,channel :: pid() | undefined
               }).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

get_channel() ->
    gen_server:call(?SERVER, channel).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes the server
%%
%% @spec init(Args) -> {ok, State} |
%%                     {ok, State, Timeout} |
%%                     ignore |
%%                     {stop, Reason}
%% @end
%%--------------------------------------------------------------------
init([]) ->
    DefaultParama = #amqp_params_network{},

    Settings = application:get_all_env(amqp_client),
    Host = proplists:get_value(host, Settings, DefaultParama#amqp_params_network.host),
    Port = proplists:get_value(port, Settings, DefaultParama#amqp_params_network.port),
    Ssl = proplists:get_value(ssl, Settings, DefaultParama#amqp_params_network.ssl_options),

    Username = proplists:get_value(username, Settings, DefaultParama#amqp_params_network.username),
    Password = proplists:get_value(password, Settings, DefaultParama#amqp_params_network.password),

    VHost = proplists:get_value(virtual_host, Settings, DefaultParama#amqp_params_network.virtual_host),
    Heartbeat = proplists:get_value(heartbeat, Settings, DefaultParama#amqp_params_network.heartbeat),

    AmqpParams = #amqp_params_network{
                    host = Host,
                    port = Port,
                    ssl_options = Ssl,
                    username = Username,
                    password = Password,
                    virtual_host = VHost,
                    heartbeat = Heartbeat
                   },
    {ok, Connection, Channel} = connect(AmqpParams),

    declare_exchange(Channel, <<"OrderBookTop">>, <<"topic">>),
    declare_exchange(Channel, <<"Pairs">>, <<"fanout">>),
    declare_exchange(Channel, <<"Admin">>, <<"fanout">>),
    declare_exchange(Channel, <<"Trades">>, <<"topic">>),

    {ok, #state{amqp_params = AmqpParams
               ,connection = Connection
               ,channel = Channel
               }}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%%
%% @spec handle_call(Request, From, State) ->
%%                                   {reply, Reply, State} |
%%                                   {reply, Reply, State, Timeout} |
%%                                   {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, Reply, State} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_call(channel, _From, #state{connection = Connection} = State) ->
    Channel = amqp_connection:open_channel(Connection),
    {reply, Channel, State};
handle_call(_Request, _From, State) ->
    lager:warning("Wrong message: ~p in module ~p state ~p", [_Request, ?MODULE, State]),
    Reply = ok,
    {reply, Reply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages
%%
%% @spec handle_cast(Msg, State) -> {noreply, State} |
%%                                  {noreply, State, Timeout} |
%%                                  {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_cast(_Msg, State) ->
    lager:warning("Wrong message: ~p in module ~p state ~p", [_Msg, ?MODULE, State]),
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling all non call/cast messages
%%
%% @spec handle_info(Info, State) -> {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_info({'DOWN', _Ref, process, Connection, _Reason},
             #state{connection = Connection
                   ,amqp_params = AmqpParams
                   ,channel = Channel
                   } = State) ->
    lager:warning("Connection ~p broken, reconnecting", [Connection]),
    amqp_channel:close(Channel),
    {ok, NewConnection, NewChannel} = connect(AmqpParams),

    State#state{connection = NewConnection
               ,channel =NewChannel
               };

handle_info(_Info, State) ->
    lager:warning("Wrong message: ~p in module ~p state ~p", [_Info, ?MODULE, State]),
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_server terminates
%% with Reason. The return value is ignored.
%%
%% @spec terminate(Reason, State) -> void()
%% @end
%%--------------------------------------------------------------------
terminate(_Reason, _State) ->
    ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%%
%% @spec code_change(OldVsn, State, Extra) -> {ok, NewState}
%% @end
%%--------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
        {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
declare_exchange(Channel, ExchangeName, Type) ->
    Exchange = #'exchange.declare'{
                       exchange = ExchangeName,
                       type = Type
                       %nowait = true
                      },
    #'exchange.declare_ok'{} = amqp_channel:call(Channel, Exchange).

create_queue(Channel, QueueName) ->
    Queue = #'queue.declare'{queue = QueueName},
    #'queue.declare_ok'{} = amqp_channel:call(Channel, Queue).

bind(Channel, Exchange, Queue) ->
    bind(Channel, Exchange, Queue, <<>>).

bind(Channel, Exchange, Queue, RoutingKey) ->
    Binding = #'queue.bind'{queue       = Queue,
                            exchange    = Exchange,
                            routing_key = RoutingKey},
    #'queue.bind_ok'{} = amqp_channel:call(Channel, Binding).

subscribe_to_queue(Channel, QueueName) ->
    Subscribe = #'basic.consume'{queue = QueueName},
    #'basic.consume_ok'{consumer_tag = Tag} = amqp_channel:subscribe(Channel, Subscribe, self()),
    {ok, Tag}.

subscribe_to_queue(Channel, QueueName, PID) ->
    Subscribe = #'basic.consume'{queue = QueueName},
    #'basic.consume_ok'{consumer_tag = Tag} = amqp_channel:subscribe(Channel, Subscribe, PID),
    {ok, Tag}.

set_prefetch(Channel, Messages) ->
    QoS = #'basic.qos'{prefetch_count = Messages},
    #'basic.qos_ok'{} = amqp_channel:call(Channel, QoS).

connect(AmqpParams) ->
    case amqp_connection:start(AmqpParams) of
         {ok, Connection} ->
            {ok, Channel} = amqp_connection:open_channel(Connection),
            monitor(process, Connection),
            {ok, Connection, Channel};
        {error, E} ->
            lager:warning("Connection error: ~p, retrying after 15s", [E]),
            timer:sleep(15000),
            connect(AmqpParams)
    end.
