%%%-------------------------------------------------------------------
%%% @author ins
%%% @copyright (C) 2020, ins
%%% @doc
%%%
%%% @end
%%% Created : 2020-01-24 11:01:24.662139
%%%-------------------------------------------------------------------
-module(cryptoring_amqp).

-behaviour(gen_server).

%% Public API
-export([start_link/1
        ,buy/4
        ,sell/4
        ,balances/1
        ,add_pair_to_orderbook_top/1
        ,subscribe_to_pair_orderbook_top/2
        ,subscribe_to_pair_trades/2
        ,open_orders/1
        ,asks/2
        ,asks/3
        ,bids/2
        ,bids/3
        ,cancel_order/3
        ,sync/0
        ,sync/1
        ]).

% Internal API
-export([call/2
        ,cast/2
        ,publish/2
        ]).

%% gen_server callbacks
-export([init/1
        ,handle_call/3
        ,handle_cast/2
        ,handle_info/2
        ,terminate/2
        ,code_change/3
        ]).

-define(SERVER, ?MODULE).

-include("cryptoring_amqp.hrl").

% Callbacks
-callback dispatch(map()) -> {ok, map()} | {error, term()}.

-record(state, {channel :: pid() | undefined
               ,consumer_tag :: binary() | undefined
               ,application :: binary()
               ,callback :: module()
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
start_link(Callback) ->
    {ok, App} = application:get_application(),
    gen_server:start_link({local, ?SERVER}, ?MODULE, [atom_to_binary(App, utf8), Callback], []).

buy(Exchange, Pair, Price, Amount) ->
    Trade = application:get_env(cryptoring_amqp, trade, true),
    buy(Exchange, Pair, Price, Amount, Trade).

buy(Exchange, Pair, Price, Amount, Trade) ->
    Data = #{
      <<"action">> => <<"buy">>,
      <<"pair">> => Pair,
      <<"price">> => Price,
      <<"exchange">> => Exchange,
      <<"amount">> => Amount
     },

    case Trade of
        true ->
            cast(Exchange, Data),
            cryptoring_amqp_log:log(<<"info">>, Data#{<<"test">> => false});
        _ ->
            cast(<<Exchange/bytes, ":test">>, Data),
            cryptoring_amqp_log:log(<<"info">>, Data#{<<"test">> => true})
    end.

sell(Exchange, Pair, Price, Amount) ->
    Trade = application:get_env(cryptoring_amqp, trade, true),
    sell(Exchange, Pair, Price, Amount, Trade).

sell(Exchange, Pair, Price, Amount, Trade) ->
    Data = #{
      <<"action">> => <<"sell">>,
      <<"pair">> => Pair,
      <<"price">> => Price,
      <<"exchange">> => Exchange,
      <<"amount">> => Amount
     },

    case Trade of
        true ->
            cast(Exchange, Data),
            cryptoring_amqp_log:log(<<"info">>, Data#{<<"test">> => false});
        _ ->
            cast(<<Exchange/bytes, ":test">>, Data),
            cryptoring_amqp_log:log(<<"info">>, Data#{<<"test">> => true})
    end.

balances(Exchange) ->
    call(Exchange, #{<<"action">> => <<"balances">>}).

add_pair_to_orderbook_top(Pair) ->
    Data = #{
      <<"action">> => <<"subscribe">>,
      <<"pair">> => Pair
     },
    publish(<<"Pairs">>, Data).

subscribe_to_pair_orderbook_top(Pair, Exchange) ->
    cryptoring_amqp_queue_sup:add_queue_subscription(<<"OrderBookTop">>, <<Pair/bytes, ":", Exchange/bytes>>).

subscribe_to_pair_trades(Pair, Exchange) ->
    cryptoring_amqp_queue_sup:add_queue_subscription(<<"Trades">>, <<Pair/bytes, ":", Exchange/bytes>>).

-spec open_orders(exchange()) -> [order()].
open_orders(Exchange) ->
    call(Exchange, #{<<"action">> => <<"open_orders">>}).

-spec cancel_order(exchange(), pair(), order_id()) -> ok.
cancel_order(Exchange, Pair, OrderId) ->
    cast(Exchange, #{<<"action">> => <<"cancel_order">>
                     ,<<"id">> => OrderId
                     ,<<"pair">> => Pair
                    }).

-spec asks(exchange(), currency()) -> [#{}].
asks(Exchange, Currency) ->
    call(Exchange, #{<<"action">> => <<"asks">>
                    ,<<"currency">> => Currency
                    ,<<"limit">> => 1
                    }).

-spec asks(exchange(), currency(), non_neg_integer()) -> [#{}].
asks(Exchange, Currency, Limit) ->
    call(Exchange, #{<<"action">> => <<"asks">>
                    ,<<"currency">> => Currency
                    ,<<"limit">> => Limit
                    }).

-spec bids(exchange(), currency()) -> [#{}].
bids(Exchange, Currency) ->
    call(Exchange, #{<<"action">> => <<"bids">>
                    ,<<"currency">> => Currency
                    ,<<"limit">> => 1
                    }).

-spec bids(exchange(), currency(), non_neg_integer()) -> [#{}].
bids(Exchange, Currency, Limit) ->
    call(Exchange, #{<<"action">> => <<"bids">>
                    ,<<"currency">> => Currency
                    ,<<"limit">> => Limit
                    }).

-spec sync() -> ok.
sync() ->
    publish(<<"Admin">>, #{<<"action">> => <<"sync">>}).

-spec sync(exchange()) -> ok.
sync(Receiver) ->
    cast(Receiver, #{<<"action">> => <<"sync">>}).

cast(Reciever, Data) ->
    gen_server:cast(?MODULE, {Reciever, Data}).

call(Reciever, Data) ->
    gen_server:call(?MODULE, {Reciever, Data}, infinity).

publish(Exchange, Data) ->
    gen_server:cast(?SERVER, {publish, Exchange, Data}).

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
init([App, Callback]) ->
    {ok, Channel} = cryptoring_amqp_connection:get_channel(),

    cryptoring_amqp_connection:create_queue(Channel, App),
    cryptoring_amqp_connection:bind(Channel, <<"Admin">>, App),
    {ok, Tag} = cryptoring_amqp_connection:subscribe_to_queue(Channel, App),

    {ok, #state{channel = Channel
               ,consumer_tag = Tag
               ,application = App
               ,callback = Callback
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
handle_call({Receiver, Data},
            _From, 
            #state{
               channel = Channel,
               application = App
              } = State) ->
    CID = base64:encode(crypto:strong_rand_bytes(20)),
    Props = #'P_basic'{
               reply_to = App,
               correlation_id = CID
              },
    Msg = #amqp_msg{
             props = Props,
             payload = jsx:encode(add_ts(Data))
            },
    Request = #'basic.publish'{routing_key=Receiver},

    amqp_channel:cast(Channel, Request, Msg),

    receive
        {#'basic.deliver'{delivery_tag = Tag},
         #amqp_msg{
            props = #'P_basic'{correlation_id = CID},
            payload = Response
           }} ->
            Reply = jsx:decode(Response, [return_maps]),
            amqp_channel:cast(Channel, #'basic.ack'{delivery_tag = Tag}),
            {reply, Reply, State}
    after ?AMQP_REPLY_TIMEOUT ->
              lager:warning("Timeout in AMQP API call ~s to ~s: ~p", [CID, Receiver, Msg]),
              {reply, #{}, State}
    end;
handle_call(_Request, _From, State) ->
    lager:warning("Wrong message: ~p from ~p in module ~p state ~p", [_Request, _From, ?MODULE, State]),
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
handle_cast({publish, Exchange, Data},
            #state{
               channel = Channel
            } = State) ->
    Msg = #amqp_msg{payload = jsx:encode(add_ts(Data))},
    Response = #'basic.publish'{exchange=Exchange},

    amqp_channel:cast(Channel, Response, Msg),
    {noreply, State};
handle_cast({Receiver, Data},
            #state{
               channel = Channel
            } = State) ->
    Msg = #amqp_msg{payload = jsx:encode(add_ts(Data))},
    Response = #'basic.publish'{routing_key=Receiver},

    amqp_channel:cast(Channel, Response, Msg),
    {noreply, State};
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
handle_info({#'basic.deliver'{delivery_tag = Tag},
             #amqp_msg{
                props = #'P_basic'{correlation_id = CID
                                   ,reply_to = Reply
                                  },
                payload = Data
               }},
            #state{callback = Callback
                   ,channel = Channel
                  } = State) ->
    lager:info("AMQP cast message ~p: ~p", [Tag, Data]),
    try
        Json = jsx:decode(Data, [return_maps]),
        case Callback:dispatch(Json) of
            {error, Err} ->
                lager:warning("Error dispatching AMQP message ~p: ~p",
                              [Tag, Err]),
                amqp_channel:cast(Channel, #'basic.nack'{delivery_tag = Tag}),
                {noreply, State};
            {ok, _Response} when Reply == undefined ->
                amqp_channel:cast(Channel, #'basic.ack'{delivery_tag = Tag}),
                {noreply, State};
            {ok, Result} ->
                Props = #'P_basic'{correlation_id = CID},
                Msg = #amqp_msg{props = Props
                               ,payload = jsx:encode(Result)
                               },
                Response = #'basic.publish'{routing_key=Reply},
                amqp_channel:cast(Channel, Response, Msg),
                amqp_channel:cast(Channel, #'basic.ack'{delivery_tag = Tag}),
                {noreply, State}
        end
    catch
        error:badarg ->
            lager:warning("Malformed JSON recieved ~p", [Data]),
            {noreply, State};
        C:E ->
            lager:error("Unhandled exception ~p:~p in ~p", [C, E, ?MODULE]),
            {noreply, State}
    end;
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
add_ts(Data) ->
    Data#{<<"timestamp">> => erlang:system_time(millisecond)}.
