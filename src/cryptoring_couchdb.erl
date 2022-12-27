-module(cryptoring_couchdb).
-export([
         get_db/1,
         get_db/2,
         fetch_view/3,
         fetch_view/4,
         fetch_config_doc/1,
         dump/0,
         dump/1,
         migrate/0,
         migrate/1
        ]).

-define(SCHEMAS_BASE_PATH, "priv/couchdb").

get_db(Name) ->
    get_db(Name, []).

get_db(Name, _Opts) ->
    Server = get_server(),
    {ok, _DB} = couchbeam:open_db(Server, Name).

get_server() ->
    get_server([]).

get_server(Opts) ->
    Config = application:get_env(cryptoring_amqp, couchdb, []),
    Url = proplists:get_value(url, Config, <<"http://localhost:5984">>),
    Login = proplists:get_value(login, Config, undefined),
    Password = proplists:get_value(password, Config, undefined),
    lager:info("Connecting to couch: ~p with login ~s", [Url, Login]),
    Auth = maybe_auth(Login, Password, Opts),
    couchbeam:server_connection(Url, Auth).

fetch_view(DB, DDoc, View) ->
    fetch_view(DB, DDoc, View, []).

fetch_view(DB, DDoc, View, Opts) ->
    {ok, D} = couchbeam_view:fetch(DB, {DDoc, View}, Opts),
    view_to_map(D).

fetch_config_doc(DocId) ->
    {ok, DB} = get_db(<<"config">>),
    {ok, JObj} = couchbeam:open_doc(DB, DocId),
    jobj_to_map(JObj).

migrate() ->
    DBs = application:get_env(cryptoring_web, dbs, [<<"logs">>, <<"daily">>, <<"config">>]),
    migrate(DBs).

migrate(DBs) ->
    lists:foreach(fun migrate_db/1, DBs).

migrate_db(DB) ->
    Server = get_server(),
    {ok, _} = couchbeam:open_or_create_db(Server, DB),
    DBPath = filename:join([?SCHEMAS_BASE_PATH, DB]),
    {ok, DDs} = file:list_dir(DBPath),
    lists:foreach(fun(DD) -> migrte_dd(DB, DD) end, DDs).

migrte_dd(_DB, ".empty") ->
    ok;
migrte_dd(DB, DD) ->
    DDPath = filename:join([?SCHEMAS_BASE_PATH, DB, DD]),
    lager:info("Migrating path: ~s", [DDPath]),
    DDId = <<"_design/", (list_to_binary(DD))/bytes>>,
    {ok, Views} = file:list_dir(DDPath),
    {ok, DBC} = get_db(DB),
    JObj = case couchbeam:open_doc(DBC, DDId) of
               {ok, Doc} ->
                   Rev = couchbeam_doc:get_rev(Doc),
    
                   {[
                     {<<"_id">>, DDId},
                     {<<"_rev">>, Rev},
                     {<<"views">>, {[migrate_view(DB, DD, View) 
                                     || View <- Views,
                                        View /= "dd.json"]}},
                     {<<"language">>, <<"javascript">>}
                    ]};
               _ ->
                   {[
                     {<<"_id">>, <<"_design/", (list_to_binary(DD))/bytes>>},
                     {<<"views">>, {[migrate_view(DB, DD, View) 
                                     || View <- Views,
                                        View /= "dd.json"]}},
                     {<<"language">>, <<"javascript">>}
                    ]}
           end,
    lager:info("JObj: ~p", [JObj]),
    {ok, JObj1} = couchbeam:save_doc(DBC, JObj),
    file:write_file(filename:join(DDPath, "dd.json"), couchbeam_ejson:encode(JObj1)).
    

migrate_view(DB, DD, View) ->
    ViewPath = filename:join([?SCHEMAS_BASE_PATH, DB, DD, View]),
    {ok, Funs} = file:list_dir(ViewPath),
    {list_to_binary(View), {[{list_to_binary(Name), Data} || Fun <- Funs,
                                     {ok, Data} <- [file:read_file(filename:join(ViewPath, Fun))],
                                     [Name, _] <- [string:split(Fun, ".")]
    ]}}.

dump() ->
    DBs = application:get_env(cryptoring_web, dbs, [<<"logs">>, <<"daily">>, <<"config">>]),
    dump(DBs).

dump(DBs) ->
    lists:foreach(fun dump_db/1, DBs).

dump_db(DB) ->
    file:make_dir(filename:join(?SCHEMAS_BASE_PATH, DB)),
    {ok, Connection} = get_db(DB),
    {ok, JObj} = couchbeam:open_doc(Connection, <<"_design_docs">>),
    DDs = [ DD || #{<<"rows">> := Rows} <- [jobj_to_map(JObj)],
                  #{<<"id">> := DD} <- Rows ],
    lists:foreach(fun(DD) -> dump_dd(DB, DD) end, DDs).

dump_dd(DB, <<"_design/", DDName/bytes>> = DD) ->
    DDPath = filename:join([?SCHEMAS_BASE_PATH, DB, DDName]),
    file:make_dir(DDPath),
    lager:info("Design Doc ID = ~p", [DD]),

    {ok, Connection} = get_db(DB),
    {ok, JObj} = couchbeam:open_doc(Connection, DD), 
    #{<<"views">> := Views} = Doc = jobj_to_map(JObj),
    file:write_file(filename:join(DDPath, "dd.json"), jsx:encode(Doc)),
    maps:fold(fun dump_view/3, DDPath, Views).

dump_view(Name, Data, DDPath) ->
    Path = filename:join(DDPath, Name),
    file:make_dir(Path),

    case maps:get(<<"map">>, Data, undefined) of
        undefined ->
            ok;
        Map ->
            file:write_file(filename:join(Path, <<"map.js">>), Map)
    end,

    case maps:get(<<"reduce">>, Data, undefined) of
        undefined ->
            ok;
        Reduce ->
            file:write_file(filename:join(Path, <<"reduce.js">>), Reduce)
    end,
    DDPath.

view_to_map(View) ->
    view_to_map(View, []).

view_to_map(View, _Opts) ->
    lists:foldl(
      fun({PL}, Acc) ->
              Keys = maybe_compile_date(proplists:get_value(<<"key">>, PL, [])),
              V = proplists:get_value(<<"value">>, PL, {[]}),
              Doc = proplists:get_value(<<"doc">>, PL, {[]}),
              update_deep_keys(Keys, Acc, #{<<"value">> => jobj_to_map(V),
                                            <<"doc">> => jobj_to_map(Doc)})
      end,
      #{},
      View).

maybe_compile_date([Y, M, D | Ks]) when is_integer(Y),
                                        is_integer(M),
                                        is_integer(D) ->
    Date = wf:f(<<"~p-~2.10.0B-~2.10.0B">>, [Y, M, D]),
    [Date | Ks];
maybe_compile_date(Ks) ->
    Ks.

jobj_to_map({PL}) when is_list(PL) ->
    lists:foldl(fun({K, V}, Map) ->
                        Map#{K => jobj_to_map(V)}
                end,
                #{},
                PL);
jobj_to_map(L) when is_list(L) ->
    [jobj_to_map(LI) || LI <- L];
jobj_to_map(Any) ->
    Any.

update_deep_keys([K], Map, V) ->
    Map#{K => V};
update_deep_keys([K | T], Map, V) ->
    maps:update_with(K, 
                     fun(Old) ->
                             update_deep_keys(T, Old, V)
                     end,
                     update_deep_keys(T, #{}, V),
                     Map).

maybe_auth(undefined, _, Opts) -> Opts;
maybe_auth(_, undefined, Opts) -> Opts;
maybe_auth(Login, Password, Opts) -> 
    [{basic_auth, {Login, Password}} | Opts].
