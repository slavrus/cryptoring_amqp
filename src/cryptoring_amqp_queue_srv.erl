%%%-------------------------------------------------------------------
%%% @author ins
%%% @copyright (C) 2020, ins
%%% @doc
%%%
%%% @end
%%% Created : 2020-01-28 09:34:15.165937
%%%-------------------------------------------------------------------
-module(cryptoring_amqp_queue_srv).

-behaviour(gen_server).

%% API
-export([
         start_link/3
        ]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-define(SERVER, ?MODULE).
-include("cryptoring_amqp.hrl").

-record(state, {
          channel :: pid() | undefined,
          consumer_tag :: binary() | undefined,
          receiver :: pid(),
          queue :: binary() | undefined,
          exchange :: binary() | undefined,
          timeout = 60000 :: non_neg_integer()
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
start_link(Pid, Exchange, RoutingKey) ->
    gen_server:start_link(?MODULE, [Pid, Exchange, RoutingKey], []).

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
init([Pid, Exchange, RoutingKey]) ->
    Timeout = application:get_env(cryptoring_amqp, queue_inactivity_timeout, 60000),
    [_, CryptoExchange] = binary:split(RoutingKey, <<":">>),
    {ok, Channel} = cryptoring_amqp_connection:get_channel(),
    Queue = iolist_to_binary(io_lib:format("~s-~p", [RoutingKey, self()])),
    create_queue(Channel, Queue),
    cryptoring_amqp_connection:bind(Channel, Exchange, Queue, RoutingKey),
    cryptoring_amqp_connection:set_prefetch(Channel, 1),
    {ok, Tag} = cryptoring_amqp_connection:subscribe_to_queue(Channel, Queue),
    {ok, #state{
            consumer_tag = Tag,
            queue = Queue,
            channel = Channel,
            exchange = CryptoExchange,
            timeout = Timeout,
            receiver = Pid
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
handle_call(_Request, _From, State) ->
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
handle_info(#'basic.consume_ok'{}, State) ->
    {noreply, State};
handle_info(#'basic.cancel_ok'{}, State) ->
    {noreply, State};
handle_info({#'basic.deliver'{delivery_tag = Tag}, 
             #amqp_msg{
                payload = Data
               }
            },
            #state{
               channel = Channel,
               timeout = Timeout,
               receiver = Receiver
              } = State) ->
    lager:debug("AMQP cast message: ~p", [Data]),
    Json = jsx:decode(Data, [return_maps]),
    gen_statem:call(Receiver, Json),
    amqp_channel:cast(Channel, #'basic.ack'{delivery_tag = Tag}),
    {noreply, State, Timeout};
handle_info(timeout,
            #state{
               timeout = Timeout,
               exchange = Exchange,
               receiver = Receiver
              } = State) ->
    lager:warning("No new data for ~s within ~ps sending notification",
                  [Exchange, Timeout / 1000]),
    Json = #{
      <<"error">> => <<"timeout">>,
      <<"exchange">> => Exchange
     },
    Receiver ! Json,
    {noreply, State};
handle_info(_Info, State) ->
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
create_queue(Channel, QueueName) ->
    Queue = #'queue.declare'{
               queue = QueueName,
               exclusive = true,
               auto_delete = true,
               arguments = [
                            {<<"x-max-length">>, long, 1},
                            {<<"x-overflow">>, longstr, <<"drop-head">>}
                           ]
              },
    #'queue.declare_ok'{} = amqp_channel:call(Channel, Queue).
