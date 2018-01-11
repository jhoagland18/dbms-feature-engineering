in(X,[X|_]).
in(X,[_|R]) :- in(X,R).

% the function bin_boundaries returns all X,Y bin boundaries for a given numeric attribute
bin_boundaries(X,Y,Table,Attribute) :-
	bin_thresholds(Table,Attribute,L),
	bin_boundaries(X,Y,L).
	
bin_boundaries(X,Y,[A,B|T]) :-
	(X=A,Y=B);
	bin_boundaries(X,Y,[B|T]).


%% TIME_STAMP MANAGEMENT
time_stamp_in_both_tables(T,U):-
	attribute(T,_,timestamp,_),
	attribute(U,_,timestamp,_).

% if T0 of type T and T1 of type U have both timestamps, Out=AND T.Timestamp < U.Timestamp, otherwise, Out=""
time_stamp_condition(T,U,T0,T1,Out) :-
	attribute(T,A,timestamp,_),
	attribute(U,B,timestamp,_),
	atomic_list_concat(['AND ',T0,'.',A,' < ',T1,'.',B],Out).

time_stamp_condition(T,U,_,_,Out) :-
	\+time_stamp_in_both_tables(T,U),
	Out=''.
	
%%% PK MANAGEMENT %%%%
% out will be t.PK0, t.PK1, ...
sql_unpackPK(Table,T,Out) :-
	pk(Table,PKs),
	sql_unpackPK(T,PKs,'',Out).

sql_unpackPK(T,[PK],CurString,Out) :-
	atomic_list_concat([CurString,T,'.',PK],Out).

sql_unpackPK(T,[PK|N],CurString,Out) :-
	atomic_list_concat([CurString,T,'.',PK,', '],S4),
	sql_unpackPK(T,N,S4,Out).

%%% JOIN MANAGEMENT %%%%
% out will be t1.FK1_0 = t2.FK1_0 AND t1.FK1_1 = t2.FK1_1 AND  ...
sql_ON(T1,T2,[FK1],[FK2],CurString,Out) :-
	atomic_list_concat([CurString,T1,'.',FK1,' = ', T2, '.', FK2],Out).

sql_ON(T1,T2,[FK1|R1],[FK2|R2],CurString,Out) :-
	atomic_list_concat([CurString,T1,'.',FK1,' = ',T2,'.',FK2,' AND '],S1),
	sql_ON(T1,T2,R1,R2,S1,Out).

%%% WHERE TO-VALUE CONDITIONS %%%
%0-1 variable. Out will be "WHERE tableVarName.Att = 0/1"
where_cond(Table,TableVarName,Att,And,Out) :-
	attribute(Table,Att,zero_one,_),
	member(N,[0,1]),
	((And=1,FirstWord='AND ');(And=0,FirstWord='WHERE ')),
	atomic_list_concat([FirstWord,TableVarName,'.',Att,' = ',N],Out).

%nominal variable
where_cond(Table,TableVarName,Att,And,Out) :-
	attribute(Table,Att,nominal,_),
	important_values(Table,Att,ImportantValues),
	member(N,ImportantValues),
	((And=1,FirstWord='AND ');(And=0,FirstWord='WHERE ')),
	atomic_list_concat([FirstWord,TableVarName,'.',Att,' = ','\'',N,'\''],Out).

%numeric variable: WHERE tableVarName.Att > X AND tableVarName.Att < Y for each binthresholds X,Y 
where_cond(Table,TableVarName,Att,And,Out) :-
	attribute(Table,Att,numeric,_),
	bin_boundaries(X,Y,Table,Att),
	((And=1,FirstWord='AND ');(And=0,FirstWord='WHERE ')),
	atomic_list_concat([FirstWord,TableVarName,'.',Att,' > ',X,' AND ',TableVarName,'.',Att,' < ',Y],Out).


