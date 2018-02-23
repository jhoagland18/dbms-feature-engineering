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


pk('BinarizedRatings',['companyid']).
attribute('BinarizedRatings', 'Broad Bean_Origin', 'nominal', 'null').
important_values('BinarizedRatings','Broad Bean_Origin',['','South America','Papua New Guinea','Sao Tome & Principe','"Trinidad, Tobago"','"Cost Rica, Ven"','Grenada','Solomon Islands','Trinidad','El Salvador','Panama','"Ghana, Domin. Rep"','Brazil','Guatemala',' ','Ecuador','Colombia','Tobago','Carribean','Tanzania','"Peru, Madagascar"','Domincan Republic','Belize','Ghana','"Dom. Rep., Madagascar"','"Gre., PNG, Haw., Haiti, Mad"','"Colombia, Ecuador"','Congo','India','Vanuatu','"Venezuela, Java"','Central and S. America','Ghana & Madagascar','Honduras','"PNG, Vanuatu, Mad"','Jamaica','Peru','"Peru, Dom. Rep"','"DR, Ecuador, Peru"','Haiti','"Peru, Belize"','Puerto Rico','Fiji','"Ven.,Ecu.,Peru,Nic."','St. Lucia','Madagascar','Ivory Coast','Bolivia','Costa Rica','Vietnam','Martinique','Trinidad-Tobago','Sao Tome','Togo','Sri Lanka','Philippines','Hawaii','"Venez,Africa,Brasil,Peru,Mex"','"Ven, Bolivia, D.R."','"Guat., D.R., Peru, Mad., PNG"','Samoa','"Africa, Carribean, C. Am."','Principe','Venezuela','Liberia','Cuba','"Venezuela, Dom. Rep."','Nicaragua','West Africa','Dominican Republic','Mexico','Uganda','"Dominican Rep., Bali"','Australia','"Venezuela, Trinidad"','Indonesia']).
attribute('BinarizedRatings', 'RatingID', 'numeric', 'null').
bin_thresholds('BinarizedRatings','RatingID',[1.0,180.4,359.8,539.2,718.6,898.0,1077.4,1256.8,1436.2,1615.6000000000001]).
attribute('BinarizedRatings', 'Cocoa_Percent', 'numeric', 'null').
bin_thresholds('BinarizedRatings','Cocoa_Percent',[0.42,0.478,0.536,0.5940000000000001,0.652,0.71,0.768,0.8260000000000001,0.8840000000000001,0.9420000000000002]).
attribute('BinarizedRatings', 'Bean_Type', 'nominal', 'null').
important_values('BinarizedRatings','Bean_Type',['','Nacional','Nacional (Arriba)','Forastero (Arriba)','"Trinitario, Criollo"','"Trinitario, TCGA"','Trinitario (85% Criollo)','"Trinitario, Nacional"','Forastero (Nacional)','Trinitario','Criollo',' ','Blend','Forastero (Amelonado)','Criollo (Ocumare 77)','Criollo (Porcelana)','Criollo (Ocumare 67)','"Criollo, Forastero"','Matina','Beniano','"Criollo, Trinitario"','Criollo (Wild)','"Blend-Forastero,Criollo"','EET','Forastero','"Amazon, ICS"','Forastero (Arriba) ASS','Forastero (Parazinho)','Amazon mix']).
attribute('BinarizedRatings', 'Rating', 'numeric', 'null').
bin_thresholds('BinarizedRatings','Rating',[1.0,1.4,1.8,2.2,2.6,3.0,3.4000000000000004,3.8000000000000003,4.2,4.6]).
attribute('BinarizedRatings', 'Review_Date', 'numeric', 'null').
bin_thresholds('BinarizedRatings','Review_Date',[2006.0,2007.1,2008.2,2009.3,2010.4,2011.5,2012.6,2013.7,2014.8,2015.9]).
attribute('BinarizedRatings', 'NewRating', 'numeric', 'null').
bin_thresholds('BinarizedRatings','NewRating',[0.0,0.1,0.2,0.30000000000000004,0.4,0.5,0.6000000000000001,0.7000000000000001,0.8,0.9]).
attribute('BinarizedRatings', 'TimeStamp', 'timestamp', 'timestamp').
pk('Locations',['Company_location']).
pk('Company',['companyid','company_location']).
attribute('Company', 'Company', 'nominal', 'null').
important_values('Company','Company',['Bisou','Condor','Dolfin (Belcolade)','Peppalo','Brazen','Szanto Tibor','Kto','Whittakers','DeVries','Xocolla','Majani','Madecasse (Cinagra)','Nuance','Glennmade','Hachez','Heirloom Cacao Preservation (Brasstown)','Woodblock','Shattel','Indah','Robert (aka Chocolaterie Robert)','Spagnvola','Callebaut','Desbarres','Fruition','Ohiyo','Menakao (aka Cinagra)','Just Good Chocolate','ENNA','Chocolate Con Amor','Lilla','Black River (A. Morin)','Original Beans (Felchlin)','La Pepa de Oro','Ethel's Artisan (Mars)','Solomons Gold','Roasting Masters','Chocovivo','C-Amaro','Somerville','Svenska Kakaobolaget','Tablette (aka Vanillabeans)','Pierre Marcolini','Beschle (Felchlin)','Luker','Arete','Castronovo','Fossa','Raaka','Original Hawaiin Chocolate Factory','Omnom','Altus aka Cao Artisan','Adi','hexx','Dulcinea','Maglio','Raw Cocoa','Amatller (Simon Coll)','Cacao de Origen','Cemoi','Matale','Ritual','Indi','Green & Black's (ICAM)','Shark Mountain','Loiza','Malmo','Forteza (Cortes)','Grenada Chocolate Co.','Dark Forest','Ocelot','Chocola'te','Cacaoyere (Ecuatoriana)','Amazona','Monsieur Truffe','Bahen & Co.','Georgia Ramon','Akesson's (Pralus)','Pump Street Bakery','Batch','Omanhene','Lindt & Sprungli','Tobago Estate (Pralus)','Choklat','iQ Chocolate','Domori','twenty-four blackbirds','Montecristi','Manoa','Felchlin','Britarev','The Barn','Kakao','K'ul','Cello','Beau Cacao','Wellington Chocolate Factory','Holy Cacao','Friis Holm','Frederic Blondeel','Dalloway','Chocolats Privilege','Pinellas','Fearless (AMMA)','Cacao Prieto','De Mendes','Christopher Morel (Felchlin)','Rogue','Mita','Feitoria Cacao','Askinosie','Bonnat','Belcolade','East Van Roasters','Wilkie's Organic','Cacao Barry','Nibble','Vanleer (Barry Callebaut)','StRita Supreme','Pura Delizia','Hotel Chocolat (Coppeneur)','Burnt Fork Bend','Mutari','Artisan du Chocolat (Casa Luker)','Black Mountain','Tejas','Malagos','Belyzium','Raoul Boulanger','Spencer','Caribeans','Manifesto Cacao','Kyya','Soma','Goodnow Farms','Neuhaus (Callebaut)','Sirene','Park 75','Chaleur B','Grand Place','Choco Del Sol','Violet Sky','Vintage Plantations (Tulicorp)','Map Chocolate','Chocovic','Marsatta','Ah Cacao','El Ceibo','Jordis','L.A. Burdick (Felchlin)','Creo','Cacao Store','Tocoti','Daintree','Edelmond','Oakland Chocolate Co.','Sibu','hello cocoa','Fresco','Blanxart','Lillie Belle','Upchurch','Zokoko','Finca','Molucca','Momotombo','Nugali','La Chocolaterie Nanairo','Rain Republic','Davis','Emily's','Vivra','Chocablog','"Smooth Chocolator, The"','Indaphoria','Blue Bandana','Carlotta Chocolat','Escazu','Millcreek Cacao Roasters','Un Dimanche A Paris','Cacao Sampaka','Suruca Chocolate','Metiisto','Valrhona','Solstice','Damson','Stella (aka Bernrain)','Charm School','Lonohana','Chokolat Elot (Girard)','Guido Castagna','Potomac','Terroir','"Chocolate Tree, The"','Lake Champlain (Callebaut)','Amano','Olive and Sinclair','Tan Ban Skrati','Dick Taylor','Ki' Xocolatl','Mayacama','Choocsol','Ocho','Noble Bean aka Jerjobo','Tsara (Cinagra)','Pitch Dark','Palette de Bine','Bar Au Chocolat','Guittard','Chocolate Conspiracy','Sjolinds','Snake & Butterfly','Salgado','Durci','Cote d' Or (Kraft)','Eclat (Felchlin)','Chloe Chocolat','Brasstown aka It's Chocolate','Seaforth','ChocoReko','Duffy's','Quetzalli (Wolter)','Machu Picchu Trading Co.','Shark's','Theo','Claudio Corallo','Letterpress','Metropolitan','Chchukululu (Tulicorp)','Kerchner','Idilio (Felchlin)','Hoja Verde (Tulicorp)','Alain Ducasse','Chuao Chocolatier (Pralus)','Heirloom Cacao Preservation (Manoa)','Harper Macaw','Ara','Soul','Levy','Nahua','Silvio Bessone','Captain Pembleton','Emerald Estate','Chuao Chocolatier','Mana','Habitual','Coppeneur','Rozsavolgyi','Monarque','Starchild','To'ak (Ecuatoriana)','Erithaj (A. Morin)','Vicuna','Taza','Mindo','Confluence','Chocolarder','Timo A. Meyer','Chocolate Makers','Naive','Cacao Market','Benoit Nihant','Rancho San Jacinto','Moho','El Rey','Meadowlands','Olivia','Vintage Plantations','Ambrosia','Danta','Marigold's Finest','Cravve','Bronx Grrl Chocolate','Urzi','Hogarth','Artisan du Chocolat','Hummingbird','Franceschi','Cacao Arabuco','Chequessett','Solkiki','Acalli','L'Amourette','Doble & Bignall','Mast Brothers','Orquidea','French Broad','Bittersweet Origins','Heilemann','Tabal','Nanea','Garden Island','Pacari','Shattell','Parliament','Kah Kow','Zart Pralinen','Cloudforest','Xocolat','Pascha','Mars','Eau de Rose','Honest','Amedei','A. Morin','SRSLY','Noir d' Ebine','AMMA','Pomm (aka Dead Dog)','Heirloom Cacao Preservation (Guittard)','Ethereal','Heirloom Cacao Preservation (Mindo)','Kiskadee','Bakau','Choco Dong','Willie's Cacao','Martin Mayer','Undone','Bouga Cacao (Tulicorp)','Wm','Durand','Kallari (Ecuatoriana)','Animas','Madre','Q Chocolate','Cacao de Origin','Dole (Guittard)','Zak's','Middlebury','Heirloom Cacao Preservation (Fruition)','Cacao Hunters','Scharffen Berger','Dandelion','Nathan Miller','Marana','Minimal','Michel Cluizel','Two Ravens','Lajedo do Ouro','S.A.I.D.','Alexandre','Green Bean to Bar','Santander (Compania Nacional)','Bernachon','Friis Holm (Bonnat)','La Oroquidea','Izard','Vietcacao (A. Morin)','Laia aka Chat-Noir','Enric Rovira (Claudio Corallo)','Anahata','DAR','La Maison du Chocolat (Valrhona)','Haigh','Forever Cacao','Breeze Mill','Videri','Santome','Bellflower','Ranger','Malagasy (Chocolaterie Robert)','Manufaktura Czekolady','Dean and Deluca (Belcolade)','Nova Monda','Malie Kai (Guittard)','Hotel Chocolat','Sol Cacao','TCHO','Sibu Sura','Coleman & Davis','Sacred','Mission','Sublime Origins','Aequare (Gianduja)','Naï¿½ve','Hacienda El Castillo','Beehive','Pangea','Jacque Torres','Sprungli (Felchlin)','Muchomas (Mesocacao)','Debauve & Gallais (Michel Cluizel)','Cao','Patric','Theobroma','Dormouse','Love Bar','Summerbird','De Villiers','Heirloom Cacao Preservation (Zokoko)','Baravelli's','Chocolate Alchemist-Philly','Oialla by Bojessen (Malmo)','Marou','Compania de Chocolate (Salgado)','Chocosol','Maverick','Obolo','Pralus','Bowler Man','Paul Young','Zotter','Heirloom Cacao Preservation (Millcreek)','Kaoka (Cemoi)','Republica del Cacao (aka Confecta)','Caoni (Tulicorp)','Cacaosuyo (Theobroma Inversiones)','Cacao Atlanta','Vao Vao (Chocolaterie Robert)','Treehouse','Isidro','Rococo (Grenada Chocolate Co.)','Stone Grindz','organicfair','Mesocacao','Night Owl']).
relationship('BinarizedRatings','Company',['companyid'],['companyid'],to1).
relationship('Locations','Company',['Company_location'],['company_location'],toN).
relationship('Company','BinarizedRatings',['companyid'],['companyid'],toN).
relationship('Company','Locations',['company_location'],['company_location'],to1).
