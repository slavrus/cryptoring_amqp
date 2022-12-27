%%%-------------------------------------------------------------------
%%% @author ins
%%% @copyright (C) 2020, ins
%%% @doc
%%%
%%% @end
%%% Created : 2020-02-13 18:36:38.037566
%%%-------------------------------------------------------------------
-module(cryptoring_amqp_log).

-behaviour(gen_server).

%% API
-export([
         start_link/0,
         log/1,
         log/2
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
          headers = [] :: [{binary(), atom(), any()}]
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
    {ok, Channel} = cryptoring_amqp_connection:get_channel(),

    Headers = case application:get_env(cryptoring_amqp, couchdb, []) of
                  [] ->
                      [];
                  Config ->
                      Login = proplists:get_value(login, Config, <<"">>),
                      Password = proplists:get_value(password, Config, <<"">>),
                      Hash = base64:encode(<<Login/bytes, ":", Password/bytes>>),
                      [{<<"Authorization">>, longstr, <<"Basic ", Hash/bytes>>}]
              end,
    {ok, #state{
            channel = Channel,
            headers = Headers
           }}.

log(Data) ->
    log(<<"info">>, Data).

log(Level, Data) ->
    {ok, App} = application:get_application(),
    gen_server:cast(?SERVER, {Level, Data, App}).

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
handle_cast({Level, Data, App},
            #state{
               channel = Channel,
               headers = Headers
            } = State) ->
    Msg = #amqp_msg{
             props = #'P_basic'{
                        content_type = <<"application/json">>,
                        headers = Headers
                       },
             payload = jsx:encode(add_ts(Data, App, Level))
            },
    Response = #'basic.publish'{
                  exchange = <<"logs">>,
                  routing_key=Level
                 },

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
add_ts(Data, App, Level) ->
    Data#{
      <<"timestamp">> => erlang:system_time(millisecond),
      <<"application">> => App,
      <<"level">> => Level
     }.
