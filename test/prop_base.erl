-module(prop_base).
-include_lib("proper/include/proper.hrl").

%%%%%%%%%%%%%%%%%%
%%% Properties %%%
%%%%%%%%%%%%%%%%%%
prop_test() ->
    ?FORALL(List, insert_remove_list_gen(),
        begin
            check_range(List)
        end).

%%%%%%%%%%%%%%%
%%% Helpers %%%
%%%%%%%%%%%%%%%
check_range(List) ->
    range_tree:init_tree(),
    range_tree:clean_store(),
    construct_index(List, 1, []).

construct_index([], _Ver, _InsertedList) ->
    true;

construct_index([{InsertList, RemoveList} | T], Ver, InsertedList) ->
    insert(InsertList, Ver),
    remove(RemoveList, Ver),
    NewInsertedList = construct_list(InsertList, RemoveList, InsertedList),
    IndexRes = range_tree:get_range(1,1000000,true,true,Ver),
%%    io:format("~n //|||||// Version is is ~p ~n", [Ver]),
%%    io:format("~n ///// the inserted list  from previous call is ~p ~n", [InsertedList]),
%%    io:format("~n + the insert list is ~p ~n", [InsertList]),
%%    io:format("~n - the remove list is ~p ~n", [RemoveList]),
%%    io:format("~n-------the list is ~p ~n", [NewInsertedList]),
%%    io:format("~n======the result range is ~p ~n", [IndexRes]),
    case compare_lists2( ordsets:to_list(ordsets:from_list(NewInsertedList)), IndexRes) of
        true ->
%%            io:format("~n|||| comparision of list is true ~n"),
            construct_index(T, Ver+1, NewInsertedList);
        false ->
            false
    end.

insert([], _Ver) ->
    ok;
insert([{RowId, Val} | T], Ver) ->
    range_tree:insert({RowId, Val, Ver}),
    insert(T, Ver).

remove([], _Ver) ->
    ok;
remove([{RowId, Val} | T], Ver) ->
    range_tree:remove(RowId, Val, Ver),
    remove(T, Ver).


construct_list([], [], List) ->
   List;
construct_list([], [{RowId, Val} | T], List) ->
    NewList = remove_from_list({Val, RowId}, List),
    construct_list([], T, NewList);
construct_list([{RowId, Val} | T], RemoveList, List) ->
    NewList = lists:sort([{Val, RowId}] ++ List),
    construct_list(T, RemoveList, NewList).

remove_from_list({_Val, _RowId}, []) ->
    [];
remove_from_list({Val, RowId}, [{ListVal, ListRow} | T]) when Val == ListVal, RowId == ListRow->
    remove_from_list({Val, RowId}, T);
remove_from_list({Val, RowId}, [{ListVal, ListRow} | T])->
    [{ListVal, ListRow}] ++ remove_from_list({Val, RowId}, T).


compare_lists2([], []) ->
    true;
compare_lists2([], [{_Val, _Rows} | _T]) ->
    true;
compare_lists2([_Val | _T], []) ->
    false;
compare_lists2([{Val, RowId} | ListT], RangeRes) ->
    case contains({Val, RowId}, RangeRes) of
        true ->
            compare_lists2(ListT, RangeRes);
        false -> false
    end.

contains({_Val, _RowId}, []) ->
    false ;
contains({Val, RowId}, [{IndexVal, Rows} | _IndexT]) when Val == IndexVal ->
    lists:member(RowId, Rows);
contains({Val, RowId}, [{_IndexVal, _Rows} | IndexT]) ->
    contains({Val, RowId}, IndexT).



%%%%%%%%%%%%%%%%%%
%%% Generators %%%
%%%%%%%%%%%%%%%%%%
insert_remove_list_gen() ->
    list({list({range(1, 1000000), range(1, 1000000)}), list({range(1, 1000000), range(1, 1000000)})}).
