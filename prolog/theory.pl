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

% if T0 of type T and T1 of type U have both timestamps, Out=AND T.Timestamp > U.Timestamp
time_stamp_condition(T,U,T0,T1,Out) :-
	attribute(T,A,timestamp,_),
	attribute(U,B,timestamp,_),
	atomic_list_concat(['AND ',T0,'.',A,' > ',T1,'.',B],Out).

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
where_cond(Table,TableVarName,Att,FirstWord,Out) :-
	attribute(Table,Att,zero_one,_),
	member(N,[0,1]),
	atomic_list_concat([FirstWord,TableVarName,'.',Att,' = ',N],Out).

%nominal variable
where_cond(Table,TableVarName,Att,FirstWord,Out) :-
	attribute(Table,Att,nominal,_),
	important_values(Table,Att,ImportantValues),
	member(N,ImportantValues),
	atomic_list_concat([FirstWord,TableVarName,'.',Att,' = ','\'',N,'\''],Out).

%numeric variable: WHERE tableVarName.Att > X AND tableVarName.Att < Y for each binthresholds X,Y 
where_cond(Table,TableVarName,Att,FirstWord,Out) :-
	attribute(Table,Att,numeric,_),
	bin_boundaries(X,Y,Table,Att),
	atomic_list_concat([FirstWord,TableVarName,'.',Att,' > ',X,' AND ',TableVarName,'.',Att,' <= ',Y],Out).


pk('CacaoBar',['cacaobarid']).
attribute('CacaoBar', 'Cocoa_Percent', 'numeric', 'percent').
bin_thresholds('CacaoBar','Cocoa_Percent',[0.0,0.5,0.6,0.7,0.8,0.9,1000.0]).
attribute('CacaoBar', 'Bean_Type', 'nominal', '').
important_values('CacaoBar','Bean_Type',['Nacional','Criollo',' Forastero','Forastero (Nacional)','Trinitario',' Forastero','Criollo (Amarru)','Trinitario',' TCGA','Criollo','Blend','Forastero (Parazinho)','Trinitario',' Criollo','Forastero(Arriba',' CCN)','CCN51','Criollo (Ocumare 61)','Criollo (Ocumare 67)','Trinitario','Criollo (Ocumare 77)','Blend-Forastero','Criollo','Criollo (Ocumare)','Criollo (Porcelana)','Criollo',' +','Forastero (Arriba) ASSS','Forastero','Matina','Amazon mix',' ','Forastero (Arriba)','Forastero (Arriba) ASS','Trinitario (85% Criollo)','','Amazon','Forastero (Catongo)','Criollo (Wild)','Forastero',' Trinitario','Criollo',' Trinitario','Beniano','Amazon',' ICS','EET','Trinitario (Scavina)','Forastero (Amelonado)','Nacional (Arriba)','Trinitario (Amelonado)','Trinitario',' Nacional']).
attribute('CacaoBar', '[Broad Bean_Origin]', 'nominal', '').
important_values('CacaoBar','[Broad Bean_Origin]',['Africa, Carribean, C. Am.','Australia','Belize','Bolivia','Brazil','Burma','Cameroon','Carribean','Carribean(DR/Jam/Tri)','Central and S. America','Colombia','Colombia, Ecuador','Congo','Cost Rica, Ven','Costa Rica','Cuba','Dom. Rep., Madagascar','Domincan Republic','Dominican Rep., Bali','Dominican Republic','DR, Ecuador, Peru','Ecuador','Ecuador, Costa Rica','Ecuador, Mad., PNG','El Salvador','Fiji','Gabon','Ghana','Ghana & Madagascar','Ghana, Domin. Rep','Ghana, Panama, Ecuador','Gre., PNG, Haw., Haiti, Mad','Grenada','Guat., D.R., Peru, Mad., PNG','Guatemala','Haiti','Hawaii','Honduras','India','Indonesia','Indonesia, Ghana','Ivory Coast','Jamaica','Liberia','Mad., Java, PNG','Madagascar','Madagascar & Ecuador','Malaysia','Martinique','Mexico','Nicaragua','Nigeria','Panama','Papua New Guinea','Peru','Peru(SMartin,Pangoa,nacional)','Peru, Belize','Peru, Dom. Rep','Peru, Ecuador','Peru, Ecuador, Venezuela','Peru, Mad., Dom. Rep.','Peru, Madagascar','Philippines','PNG, Vanuatu, Mad','Principe','Puerto Rico','Samoa','Sao Tome','Sao Tome & Principe','Solomon Islands','South America','South America, Africa','Sri Lanka','St. Lucia','Suriname','Tanzania','Tobago','Togo','Trinidad','Trinidad, Ecuador','Trinidad, Tobago','Trinidad-Tobago','Uganda','Vanuatu','Ven, Bolivia, D.R.','Ven, Trinidad, Ecuador','Ven., Indonesia, Ecuad.','Ven., Trinidad, Mad.','Ven.,Ecu.,Peru,Nic.','Venez,Africa,Brasil,Peru,Mex','Venezuela','Venezuela, Carribean','Venezuela, Dom. Rep.','Venezuela, Ghana','Venezuela, Java','Venezuela, Trinidad','Venezuela/ Ghana','Vietnam','West Africa']).
pk('Ratings',['RatingID']).
attribute('Ratings', 'TimeStamp', 'timestamp', 'timestamp').
attribute('Ratings', 'Rating', 'numeric', '').
bin_thresholds('Ratings','Rating',[0.0,1.0,2.0,3.0,4.0,5.0]).
pk('Company',['companyid']).
attribute('Company', 'company_location', 'nominal', '').
important_values('Company','company_location',['Amsterdam','Argentina','Australia','Austria','Belgium','Bolivia','Brazil','Canada','Chile','Colombia','Costa Rica','Czech Republic','Denmark','Domincan Republic','Ecuador','Eucador','Fiji','Finland','France','Germany','Ghana','Grenada','Guatemala','Honduras','Hungary','Iceland','India','Ireland','Israel','Italy','Japan','Lithuania','Madagascar','Martinique','Mexico','Netherlands','New Zealand','Niacragua','Nicaragua','Peru','Philippines','Poland','Portugal','Puerto Rico','Russia','Sao Tome','Scotland','Singapore','South Africa','South Korea','Spain','St. Lucia','Suriname','Sweden','Switzerland','U.K.','U.S.A.','Venezuela','Vietnam','Wales']).
attribute('Company', 'Company', 'nominal', '').
important_values('Company','Company',['Amsterdam','Argentina','Australia','Austria','Belgium','Bolivia','Brazil','Canada','Chile','Colombia','Costa Rica','Czech Republic','Denmark','Domincan Republic','Ecuador','Eucador','Fiji','Finland','France','Germany','Ghana','Grenada','Guatemala','Honduras','Hungary','Iceland','India','Ireland','Israel','Italy','Japan','Lithuania','Madagascar','Martinique','Mexico','Netherlands','New Zealand','Niacragua','Nicaragua','Peru','Philippines','Poland','Portugal','Puerto Rico','Russia','Sao Tome','Scotland','Singapore','South Africa','South Korea','Spain','St. Lucia','Suriname','Sweden','Switzerland','U.K.','U.S.A.','Venezuela','Vietnam','Wales']).
pk('locations',['Company_Location']).
relationship('CacaoBar','Company',['cacaobarid'],['companyid'],to1).
relationship('CacaoBar','Ratings',['cacaobarid'],['cacaobarid'],toN).
relationship('Ratings','CacaoBar',['cacaobarid'],['cacaobarid'],to1).
relationship('Company','locations',['companyid'],['null'],to1).
relationship('Company','CacaoBar',['companyid'],['cacaobarid'],toN).
relationship('locations','Company',['null'],['companyid'],toN).
