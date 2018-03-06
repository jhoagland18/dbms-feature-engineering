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


pk('Locations',['[Company_location]']).
attribute('Locations', '[Company_location]', 'nominal', 'null').
pk('BinarizedRatings',['[RatingID]']).
attribute('BinarizedRatings', '[RatingID]', 'nominal', 'null').
attribute('BinarizedRatings', '[Broad Bean_Origin]', 'nominal', 'null').
important_values('BinarizedRatings','[Broad Bean_Origin]',['South America','Papua New Guinea','Sao Tome & Principe','Trinidad, Tobago','Cost Rica, Ven','Grenada','Solomon Islands','Trinidad','El Salvador','Panama','Ghana, Domin. Rep','Guatemala','Brazil',' ','Ecuador','Colombia','Tobago','Carribean','Tanzania','Peru, Madagascar','Belize','Ghana','Domincan Republic','Gre., PNG, Haw., Haiti, Mad','Dom. Rep., Madagascar','Colombia, Ecuador','Congo','India','Vanuatu','Venezuela, Java','Central and S. America','Ghana & Madagascar','Honduras','PNG, Vanuatu, Mad','Jamaica','Peru','Peru, Dom. Rep','DR, Ecuador, Peru','Haiti','Peru, Belize','Puerto Rico','Fiji','Ven.,Ecu.,Peru,Nic.','St. Lucia','Madagascar','Ivory Coast','Bolivia','Costa Rica','Vietnam','Martinique','Trinidad-Tobago','Sao Tome','Togo','Sri Lanka','Philippines','Hawaii','Venez,Africa,Brasil,Peru,Mex','Ven, Bolivia, D.R.','Guat., D.R., Peru, Mad., PNG','Samoa','Africa, Carribean, C. Am.','Principe','Cuba','Venezuela','Venezuela, Dom. Rep.','Liberia','Nicaragua','West Africa','Dominican Republic','Uganda','Mexico','Dominican Rep., Bali','Australia','Venezuela, Trinidad','Indonesia']).
attribute('BinarizedRatings', '[Cocoa_Percent]', 'numeric', 'null').
bin_thresholds('BinarizedRatings','[Cocoa_Percent]',[0.42,0.478,0.536,0.5940000000000001,0.652,0.71,0.768,0.8260000000000001,0.8840000000000001,0.9420000000000002]).
attribute('BinarizedRatings', '[Bean_Type]', 'nominal', 'null').
important_values('BinarizedRatings','[Bean_Type]',['Nacional','Nacional (Arriba)','Forastero (Arriba)','Trinitario, Criollo','Trinitario, TCGA','Trinitario (85% Criollo)','Trinitario, Nacional','Forastero (Nacional)','Trinitario','Criollo',' ','Blend','Forastero (Amelonado)','Criollo (Ocumare 77)','Criollo (Porcelana)','Criollo (Ocumare 67)','Criollo, Forastero','Matina','Beniano','Criollo, Trinitario','Criollo (Wild)','Blend-Forastero,Criollo','EET','Forastero','Amazon, ICS','Forastero (Arriba) ASS','Amazon mix','Forastero (Parazinho)']).
attribute('BinarizedRatings', '[Rating]', 'numeric', 'null').
bin_thresholds('BinarizedRatings','[Rating]',[1.0,1.4,1.8,2.2,2.6,3.0,3.4000000000000004,3.8000000000000003,4.2,4.6]).
attribute('BinarizedRatings', '[Review_Date]', 'numeric', 'null').
bin_thresholds('BinarizedRatings','[Review_Date]',[2006.0,2007.1,2008.2,2009.3,2010.4,2011.5,2012.6,2013.7,2014.8,2015.9]).
attribute('BinarizedRatings', '[NewRating]', 'zero_one', 'zero-one').
attribute('BinarizedRatings', '[TimeStamp]', 'timestamp', 'timestamp').
pk('Company',['[companyid]']).
attribute('Company', '[companyid]', 'nominal', 'null').
attribute('Company', '[Company_location]', 'nominal', 'null').
important_values('Company','[Company_location]',['Wales','Eucador','Portugal','Iceland','Grenada','Austria','South Korea','Brazil','Guatemala','Chile','Ecuador','Colombia','Argentina','Hungary','Japan','Domincan Republic','Ghana','India','New Zealand','Canada','Belgium','Finland','South Africa','Italy','Honduras','U.S.A.','Peru','Germany','Puerto Rico','Singapore','Fiji','St. Lucia','Scotland','Madagascar','Bolivia','Costa Rica','Russia','Sweden','Niacragua','Vietnam','Netherlands','Amsterdam','Ireland','Poland','Martinique','U.K.','Lithuania','France','Sao Tome','Philippines','Switzerland','Spain','Venezuela','Czech Republic','Nicaragua','Denmark','Mexico','Suriname','Israel','Australia']).
attribute('Company', '[Company]', 'nominal', 'null').
important_values('Company','[Company]',['Condor','Bisou','Dolfin (Belcolade)','Peppalo','Brazen','Szanto Tibor','Kto','Whittakers','DeVries','Madecasse (Cinagra)','Majani','Xocolla','Bright','Nuance','Glennmade','Hachez','Heirloom Cacao Preservation (Brasstown)','Woodblock','Shattel','Indah','Robert (aka Chocolaterie Robert)','Callebaut','Spagnvola','Desbarres','Fruition','Ohiyo','Menakao (aka Cinagra)','Just Good Chocolate','ENNA','Chocolate Con Amor','Lilla','Black River (A. Morin)','Original Beans (Felchlin)','La Pepa de Oro','Ethel\'s Artisan (Mars)','Solomons Gold','Roasting Masters','Chocovivo','C-Amaro','Somerville','Svenska Kakaobolaget','Tablette (aka Vanillabeans)','Pierre Marcolini','Beschle (Felchlin)','Luker','Arete','Castronovo','Fossa','Raaka','Original Hawaiin Chocolate Factory','Omnom','Altus aka Cao Artisan','Adi','hexx','Dulcinea','Maglio','Raw Cocoa','Amatller (Simon Coll)','Cacao de Origen','Cemoi','Ritual','Indi','Matale','Green & Black\'s (ICAM)','Shark Mountain','Loiza','Forteza (Cortes)','Malmo','Grenada Chocolate Co.','Dark Forest','Ocelot','Chocola\'te','Cacaoyere (Ecuatoriana)','Amazona','Monsieur Truffe','Bahen & Co.','Georgia Ramon','Akesson\'s (Pralus)','Batch','Pump Street Bakery','Lindt & Sprungli','Omanhene','Tobago Estate (Pralus)','Choklat','iQ Chocolate','Domori','twenty-four blackbirds','Montecristi','Manoa','Felchlin','The Barn','Britarev','Kakao','K\'ul','Cello','Beau Cacao','Wellington Chocolate Factory','Holy Cacao','Friis Holm','Dalloway','Frederic Blondeel','Chocolats Privilege','Pinellas','Fearless (AMMA)','Cacao Prieto','Christopher Morel (Felchlin)','De Mendes','Mita','Rogue','Feitoria Cacao','Bonnat','Askinosie','Belcolade','East Van Roasters','Wilkie\'s Organic','Vanleer (Barry Callebaut)','Nibble','Cacao Barry','StRita Supreme','Pura Delizia','Hotel Chocolat (Coppeneur)','Burnt Fork Bend','Mutari','Artisan du Chocolat (Casa Luker)','Black Mountain','Tejas','Belyzium','Malagos','Raoul Boulanger','Spencer','Caribeans','Manifesto Cacao','Kyya','Soma','Goodnow Farms','Neuhaus (Callebaut)','Sirene','Park 75','Chaleur B','Grand Place','Choco Del Sol','Violet Sky','Vintage Plantations (Tulicorp)','Chocovic','Map Chocolate','Marsatta','El Ceibo','Ah Cacao','Creo','L.A. Burdick (Felchlin)','Jordis','Cacao Store','Daintree','Tocoti','Edelmond','Oakland Chocolate Co.','hello cocoa','Sibu','Fresco','Blanxart','Lillie Belle','Upchurch','Zokoko','Finca','Molucca','Momotombo','Nugali','La Chocolaterie Nanairo','Rain Republic','Davis','Emily\'s','Vivra','Chocablog','Smooth Chocolator, The','Indaphoria','Blue Bandana','Escazu','Carlotta Chocolat','Millcreek Cacao Roasters','Un Dimanche A Paris','Suruca Chocolate','Metiisto','Cacao Sampaka','Valrhona','Solstice','Damson','Stella (aka Bernrain)','Charm School','Lonohana','Guido Castagna','Chokolat Elot (Girard)','Potomac','Terroir','Chocolate Tree, The','Lake Champlain (Callebaut)','Amano','Olive and Sinclair','Tan Ban Skrati','Ki\' Xocolatl','Dick Taylor','Mayacama','Choocsol','Ocho','Noble Bean aka Jerjobo','Pitch Dark','Tsara (Cinagra)','Bar Au Chocolat','Palette de Bine','Guittard','Chocolate Conspiracy','Sjolinds','Snake & Butterfly','Salgado','Durci','Cote d\' Or (Kraft)','Eclat (Felchlin)','Chloe Chocolat','Brasstown aka It\'s Chocolate','Seaforth','ChocoReko','Duffy\'s','Quetzalli (Wolter)','Machu Picchu Trading Co.','Shark\'s','Theo','Letterpress','Claudio Corallo','Metropolitan','Chchukululu (Tulicorp)','Kerchner','Idilio (Felchlin)','Alain Ducasse','Hoja Verde (Tulicorp)','Heirloom Cacao Preservation (Manoa)','Chuao Chocolatier (Pralus)','Harper Macaw','Ara','Soul','Levy','Nahua','Silvio Bessone','Captain Pembleton','Emerald Estate','Chuao Chocolatier','Mana','Habitual','Coppeneur','Monarque','Starchild','Rozsavolgyi','To\'ak (Ecuatoriana)','Erithaj (A. Morin)','Vicuna','Taza','Mindo','Confluence','Chocolarder','Timo A. Meyer','Chocolate Makers','Naive','Cacao Market','Benoit Nihant','Rancho San Jacinto','El Rey','Moho','Meadowlands','Olivia','Vintage Plantations','Ambrosia','Danta','Marigold\'s Finest','Cravve','Bronx Grrl Chocolate','Urzi','Hogarth','Artisan du Chocolat','Hummingbird','Franceschi','Cacao Arabuco','Chequessett','Solkiki','Acalli','L\'Amourette','Doble & Bignall','Mast Brothers','Orquidea','French Broad','Bittersweet Origins','Heilemann','Tabal','Pacari','Garden Island','Nanea','Shattell','Parliament','Kah Kow','Zart Pralinen','Cloudforest','Xocolat','Pascha','Mars','Eau de Rose','Honest','Amedei','A. Morin','SRSLY','Noir d\' Ebine','Pomm (aka Dead Dog)','AMMA','Heirloom Cacao Preservation (Guittard)','Ethereal','Kiskadee','Heirloom Cacao Preservation (Mindo)','Bakau','Choco Dong','Willie\'s Cacao','Martin Mayer','Undone','Bouga Cacao (Tulicorp)','Wm','Durand','Kallari (Ecuatoriana)','Animas','Q Chocolate','Dole (Guittard)','Cacao de Origin','Zak\'s','Middlebury','Heirloom Cacao Preservation (Fruition)','Cacao Hunters','Scharffen Berger','Nathan Miller','Dandelion','Marana','Minimal','Michel Cluizel','Two Ravens','Lajedo do Ouro','S.A.I.D.','Alexandre','Green Bean to Bar','Santander (Compania Nacional)','Bernachon','Friis Holm (Bonnat)','La Oroquidea','Izard','Vietcacao (A. Morin)','Laia aka Chat-Noir','Anahata','Enric Rovira (Claudio Corallo)','DAR','La Maison du Chocolat (Valrhona)','Haigh','Forever Cacao','Breeze Mill','Videri','Santome','Bellflower','Ranger','Malagasy (Chocolaterie Robert)','Manufaktura Czekolady','Dean and Deluca (Belcolade)','Nova Monda','Malie Kai (Guittard)','Hotel Chocolat','Sol Cacao','TCHO','Coleman & Davis','Sibu Sura','Sacred','Mission','Sublime Origins','Aequare (Gianduja)','Naï¿½ve','Hacienda El Castillo','Beehive','Pangea','Sprungli (Felchlin)','Jacque Torres','Muchomas (Mesocacao)','Debauve & Gallais (Michel Cluizel)','Cao','Patric','Theobroma','Dormouse','Love Bar','Summerbird','De Villiers','Heirloom Cacao Preservation (Zokoko)','Baravelli\'s','Chocolate Alchemist-Philly','Marou','Oialla by Bojessen (Malmo)','Compania de Chocolate (Salgado)','Chocosol','Maverick','Obolo','Pralus','Bowler Man','Paul Young','Zotter','Heirloom Cacao Preservation (Millcreek)','Kaoka (Cemoi)','Republica del Cacao (aka Confecta)','Caoni (Tulicorp)','Cacaosuyo (Theobroma Inversiones)','Cacao Atlanta','Vao Vao (Chocolaterie Robert)','Rococo (Grenada Chocolate Co.)','Treehouse','Isidro','Stone Grindz','organicfair','Mesocacao','Night Owl']).
relationship('Locations','Company',['[Company_location]'],['[Company_location]'],toN).
relationship('BinarizedRatings','Company',['[companyid]'],['[companyid]'],to1).
relationship('Company','BinarizedRatings',['[companyid]'],['[companyid]'],toN).
relationship('Company','Locations',['[company_location]'],['[company_location]'],to1).
