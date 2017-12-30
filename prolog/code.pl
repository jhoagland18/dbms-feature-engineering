in(X,[X|_]).
in(X,[_|R]) :- in(X,R).

pk('Purchases',['Purchase_ID','SecondPK']).
pk('Clients',['Client_ID']).
pk('Products',['Product_ID']).

relationship('Purchases','Clients',['ClientID'],['ClientID'],to1).
relationship('Purchases','Products',['ProductID'],['ProductID'],to1).
relationship('Clients','Purchases',['ClientID'],['ClientID'],toN).
relationship('Products','Purchases',['ProductID'],['ProductID'],toN).

% declare attributes (Table name, att name, type, dimension)
attribute('Purchases','return',zero_one,'bool').
attribute('Purchases','online',zero_one, 'bool').
attribute('Purchases','date',timestamp,'date').
attribute('Clients','age',numeric,'years').
attribute('Clients','gender',nominal,'gender').
attribute('Products','price',numeric,'dollars').

important_values('Clients','gender',['M','F']).

bin_thresholds('Clients','age',[0,20,30,40,50,60,10000]).

% the function bin_boundaries returns all X,Y bin boundaries for a given numeric attribute
bin_boundaries(X,Y,Table,Attribute) :-
	bin_thresholds(Table,Attribute,L),
	bin_boundaries(X,Y,L).
	
bin_boundaries(X,Y,[A,B|T]) :-
	(X=A,Y=B);
	bin_boundaries(X,Y,[B|T]).

%% UTILITIES

% append a list of atoms
my_atom_concat([H|T],Out) :-
	my_atom_concat(H,T,Out).
my_atom_concat(A,[B],Out) :-
	atom_concat(A,B,Out).
my_atom_concat(A,[H|T],Out) :-
	atom_concat(A,H,S1),
	my_atom_concat(S1,T,Out).
	
%% TIME_STAMP MANAGEMENT
time_stamp_in_both_tables(T,U):-
	attribute(T,_,timestamp,_),
	attribute(U,_,timestamp,_).

% if T0 of type T and T1 of type U have both timestamps, Out=AND T.Timestamp < U.Timestamp
time_stamp_condition(T,U,T0,T1,Out) :-
	attribute(T,A,timestamp,_),
	attribute(U,B,timestamp,_),
	my_atom_concat(['AND ',T0,'.',A,' < ',T1,'.',B],Out).

time_stamp_condition(T,U,_,_,Out) :-
	\+time_stamp_in_both_tables(T,U),
	Out=''.

%%% PK MANAGEMENT %%%%
% out will be t.PK0, t.PK1, ...
sql_unpackPK(Table,T,Out) :-
	pk(Table,PKs),
	sql_unpackPK(T,PKs,'',Out).

sql_unpackPK(T,[PK],CurString,Out) :-
	my_atom_concat([CurString,T,'.',PK],Out).

sql_unpackPK(T,[PK|N],CurString,Out) :-
	my_atom_concat([CurString,T,'.',PK,', '],S4),
	sql_unpackPK(T,N,S4,Out).

%%% JOIN MANAGEMENT %%%%
% out will be t1.FK1_0 = t2.FK1_0 AND t1.FK1_1 = t2.FK1_1 AND  ...
sql_ON(T1,T2,[FK1],[FK2],CurString,Out) :-
	my_atom_concat([CurString,T1,'.',FK1,' = ', T2, '.', FK2],Out).

sql_ON(T1,T2,[FK1|R1],[FK2|R2],CurString,Out) :-
	my_atom_concat([CurString,T1,'.',FK1,' = ',T2,'.',FK2,' AND '],S1),
	sql_ON(T1,T2,R1,R2,S1,Out).

