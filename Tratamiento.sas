*1868;
proc contents data=pax_all_agreements_data out=contenido_pax_all; run;

data work.muestra;
	set pax_all_agreements_data;
	if _N_ eq 100 then output;
run;

%macro valida_missing;
	proc contents data=pax_all_agreements_data out=tmp_nom_field; run;

	%let dsid = %sysfunc(open(work.tmp_nom_field));
	%let num_ciclos = %sysfunc(attrn(&dsid ,nlobsf));
	%let rc   = %sysfunc(close(&dsid));
	
	%do i=1 %to &num_ciclos.;
		proc sql noprint;
			Select NAME into: field
			From work.tmp_nom_field
			Where monotonic() eq &i.;
		quit;

		proc sql;
			Create table work.val_&i. as
				Select "&field." as campo,
					count(*) as num_missing
				From pax_all_agreements_data
				Where &field. is missing;
		quit;
	%end;
	proc sql noprint;
		Select memname into:lista_tablas separated by " "
		From dictionary.tables
		Where index(memname,"VAL_")>0 and libname='WORK' ;
	quit;

	%let dsid = %sysfunc(open(pax_all_agreements_data));
	%let num_registros = %sysfunc(attrn(&dsid ,nlobsf));
	%let rc   = %sysfunc(close(&dsid));

	data work.validacion_missing;
		retain fecha campo total_registros num_missing porcentaje_missing;
		format porcentaje_missing percent8.2;
		set &lista_tablas.;
			fecha="202103";
			total_registros=&num_registros.;
			porcentaje_missing=num_missing/&num_registros.;
	run;

	proc delete data=&lista_tablas. 
					tmp_nom_field; 
	run;
	
%mend;
%valida_missing;

%macro agrupado_campos;
	proc contents data=pax_all_agreements_data out=tmp_nom_field; run;

	%let dsid = %sysfunc(open(work.tmp_nom_field));
	%let num_ciclos = %sysfunc(attrn(&dsid ,nlobsf));
	%let rc   = %sysfunc(close(&dsid));
	
	%do i=1 %to &num_ciclos.;
		proc sql noprint;
			Select NAME into: field
			From work.tmp_nom_field
			Where monotonic() eq &i.;
		quit;

		proc sql;
			Create table work.val_&i. as
				Select distinct "&field." as campo,
					count(distinct &field.) as num_grupos
				From pax_all_agreements_data;
		quit;
	%end;

	proc sql noprint;
		Select memname into:lista_tablas separated by " "
		From dictionary.tables
		Where index(memname,"VAL_")>0 and libname='WORK' ;
	quit;

	%let dsid = %sysfunc(open(pax_all_agreements_data));
	%let num_registros = %sysfunc(attrn(&dsid ,nlobsf));
	%let rc   = %sysfunc(close(&dsid));

	data work.validacion_missing;
		retain fecha campo total_registros num_grupos porcentaje_missing;
		format porcentaje_missing percent8.2;
		set &lista_tablas.;
			fecha="202103";
			total_registros=&num_registros.;
			porcentaje_missing=num_grupos/&num_registros.;
	run;

	proc delete data=&lista_tablas. 
					tmp_nom_field; 
	run;
	
%mend;

%agrupado_campos;

data work.campos_importantes;
	set pax_all_agreements_data;
	keep Con
		Contp
		Reg
		Dat
		Agtp
		Status
		Stage
		Loc1ISO;
run;