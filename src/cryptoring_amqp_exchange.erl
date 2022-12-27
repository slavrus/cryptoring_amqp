%%%-------------------------------------------------------------------
%%% @author ins
%%% @copyright (C) 2020, ins
%%% @doc
%%%
%%% @end
%%% Created : 2020-01-20 14:46:02.234830
%%%-------------------------------------------------------------------
-module(cryptoring_amqp_exchange).

-behaviour(gen_server).

%% API
-export([
         start_link/1,
         publish_order_top/4,
         publish_order_top/5,
         publish_trade/5,
         publish_trade/6,
         sync/1
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

% Callbacks
-callback buy(pair(), price(), amount()) -> {ok, order_id()} | {error, term()}.
-callback sell(pair(), price(), amount()) -> {ok, order_id()} | {error, term()}.
-callback balances() -> #{}. % #{ currency() => #{ <<"free">> => float(), <<"locked">> => float(), <<"total">> => float()}}
-callback subscribe_pair(pair()) -> ok | {error, term()}.
-callback open_orders() -> [order()] | {error, term()}.
-callback cancel_order(pair(), order_id()) -> ok | {error, term()}.
-callback asks(pair(), non_neg_integer()) -> [order()].
-callback bids(pair(), non_neg_integer()) -> [order()].
      
-record(state, {
          channel :: pid() | undefined,
          consumer_tag :: binary() | undefined,
          callback :: module(),
          application :: binary()
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
start_link(Module) ->
    {ok, App} = application:get_application(),
    gen_server:start_link({local, ?SERVER}, ?MODULE, [atom_to_binary(App, utf8), Module], []).

publish_order_top(Direction, Pair, Price, Amount) ->
    Timestamp = erlang:system_time(millisecond),
    publish_order_top(Direction, Pair, Price, Amount, Timestamp).

publish_order_top(Direction, Pair, Price, Amount, Timestamp) ->
    gen_server:cast(?SERVER, {order_top, Direction, Pair, Price, Amount, Timestamp}).

publish_trade(Direction, Pair, Price, Amount, Id) ->
    Timestamp = erlang:system_time(millisecond),
    publish_trade(Direction, Pair, Price, Amount, Timestamp, Id).

publish_trade(Direction, Pair, Price, Amount, Timestamp, Id) ->
    gen_server:cast(?SERVER, {trade, Id, Direction, Pair, Price, Amount, Timestamp}).
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
init([App, Module]) ->
    {ok, Channel} = cryptoring_amqp_connection:get_channel(),
    lager:debug("App: ~p", [App]),

    cryptoring_amqp_connection:create_queue(Channel, App),
    cryptoring_amqp_connection:create_queue(Channel, <<App/bytes, ":test">>),
    cryptoring_amqp_connection:bind(Channel, <<"Admin">>, App),
    cryptoring_amqp_connection:bind(Channel, <<"Pairs">>, App),
    {ok, Tag} = cryptoring_amqp_connection:subscribe_to_queue(Channel, App),

    {ok, #state{
            channel = Channel,
            consumer_tag = Tag,
            callback = Module,
            application = App
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
handle_cast({order_top, Direction, Pair, Price, Amount, Timestamp},
            #state{
               channel = Channel,
               application = App
            } = State) ->
    Data = #{
      <<"exchange">> => App,
      <<"pair">> => Pair,
      <<"direction">> => Direction,
      <<"timestamp">> => Timestamp,
      <<"price">> => Price,
      <<"amount">> => Amount
      },

    JSON = jsx:encode(Data),

    Publish = #'basic.publish'{
                 exchange = <<"OrderBookTop">>,
                 routing_key = <<Pair/bytes, ":", App/bytes>>
                },
    Msg = #amqp_msg{payload = JSON},
    amqp_channel:cast(Channel, Publish, Msg),

    {noreply, State};
handle_cast({trade, Id, Direction, Pair, Price, Amount, Timestamp},
            #state{
               channel = Channel,
               application = App
            } = State) ->
    Data = #{
      <<"trade_id">> => Id,
      <<"exchange">> => App,
      <<"pair">> => Pair,
      <<"direction">> => Direction,
      <<"timestamp">> => Timestamp,
      <<"price">> => Price,
      <<"amount">> => Amount
      },

    JSON = jsx:encode(Data),

    Publish = #'basic.publish'{
                 exchange = <<"Trades">>,
                 routing_key = <<Pair/bytes, ":", App/bytes>>
                },
    Msg = #amqp_msg{payload = JSON},
    amqp_channel:cast(Channel, Publish, Msg),

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
handle_info(#'basic.consume_ok'{}, State) ->
    {noreply, State};
handle_info(#'basic.cancel_ok'{}, State) ->
    {noreply, State};
handle_info({#'basic.deliver'{delivery_tag = Tag}, 
             #amqp_msg{
                props = #'P_basic'{
                           reply_to = undefined
                          },

                payload = Data
               }
            },
            #state{
               channel = Channel,
               callback = Callback
              } = State) ->
    lager:info("AMQP cast message: ~p", [Data]),
    decode_and_apply_api(Data, Callback),
    amqp_channel:cast(Channel, #'basic.ack'{delivery_tag = Tag}),
    {noreply, State};
handle_info({#'basic.deliver'{delivery_tag = Tag}, 
             #amqp_msg{
                props = #'P_basic'{
                           correlation_id = CID,
                           reply_to = Reply
                          },

                payload = Data
               }
            },
            #state{
               channel = Channel,
               callback = Callback
              } = State) ->
    lager:info("AMQP call reply ~p cid ~p message: ~p", [Reply, CID, Data]),

    Result = decode_and_apply_api(Data, Callback),

    Props = #'P_basic'{correlation_id = CID},
    Msg = #amqp_msg{props = Props, payload = jsx:encode(Result)},
    Response = #'basic.publish'{routing_key=Reply},

    amqp_channel:cast(Channel, Response, Msg),
    amqp_channel:cast(Channel, #'basic.ack'{delivery_tag = Tag}),
    {noreply, State};
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
decode_and_apply_api(Data, Callback) ->
    try 
        Json = jsx:decode(Data, [return_maps]),
        apply_api_method(Json, Callback)
    catch
        error:badarg ->
            lager:warning("Malformed JSON recieved ~p", [Data]),
            #{error => <<"Malformed JSON">>}
        %C:E ->
        %    lager:error("Unhandled exception ~p:~p in ~p", [C, E, ?MODULE]),
        %    #{error => io_lib:format("Unhanbled: ~p ~p", [C, E])}
    end.


apply_api_method(#{<<"action">> := <<"sync">>},
                 Callback) ->
    sync(Callback);
apply_api_method(#{<<"action">> := <<"subscribe">>,
                  <<"pair">> := Pair
                  },
                 Callback) ->
    Callback:subscribe_pair(Pair);
apply_api_method(#{<<"action">> := <<"buy">>,
                  <<"price">> := Price,
                  <<"amount">> := Amount,
                  <<"pair">> := Pair
                  },
                Callback) ->
    Callback:buy(Pair, Price, Amount);
apply_api_method(#{<<"action">> := <<"sell">>,
                  <<"price">> := Price,
                  <<"amount">> := Amount,
                  <<"pair">> := Pair
                  },
                Callback) ->
    Callback:sell(Pair, Price, Amount);
apply_api_method(#{<<"action">> := <<"balances">>},
                 Callback) ->
    Callback:balances();
apply_api_method(#{<<"action">> := <<"open_orders">>},
                 Callback) ->
    Callback:open_orders();
apply_api_method(#{<<"action">> := <<"cancel_order">>
                  ,<<"id">> := OrderId
                  ,<<"pair">> := Pair
                  },
                 Callback) ->
    Callback:cancel_order(Pair, OrderId);
apply_api_method(#{<<"action">> := <<"asks">>
                  ,<<"currency">> := Currency
                  ,<<"limit">> := Limit
                  },
                 Callback) ->
    Callback:asks(Currency, Limit);
apply_api_method(#{<<"action">> := <<"bids">>
                  ,<<"currency">> := Currency
                  ,<<"limit">> := Limit
                  },
                 Callback) ->
    Callback:bids(Currency, Limit);
apply_api_method(Data, _Callback) ->
    lager:warning("Unknown method: ~p in module ~p", [Data, ?MODULE]),
    #{error => <<"Unknown action or wrong arguments">>}.

sync(Callback) ->
    #{<<"pairs">> := Pairs} = cryptoring_couchdb:fetch_config_doc("cryptoring"),
    lists:foreach(fun Callback:subscribe_pair/1, Pairs).
