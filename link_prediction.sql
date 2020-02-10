-- you might need to have a user grant permission
-- grant execute on _SYS_AFL.PAL_LINK_PREDICT to system

create or replace procedure POLER.LINK_PREDICTION()
DEFAULT SCHEMA POLER as
begin

  --sample execution of PAL Link prediction algorithm.

   --Parameter table needed for PAL
   declare param_table table(
		"PARAM_NAME" NVARCHAR(256),
		"INT_VALUE" INTEGER,
		"DOUBLE_VALUE" DOUBLE,
		"STRING_VALUE" NVARCHAR(1000)
	);
	--Table storing the result of PAL Link Prediction algorithm
	declare res table(
		NODE1 NVARCHAR(30),
		NODE2 NVARCHAR(30),
		SCORE DOUBLE
	);
	--Mandatory parameters
	INSERT INTO :param_table VALUES ('METHOD', 3, NULL, NULL);
	INSERT INTO :param_table VALUES ('THREAD_RATIO', NULL, 0.2, NULL);

	--Only use persons and person attributes
	datatable = select SRC_ID as NODE1,
				       DST_ID as NODE2
				from RELATIONSHIP
				where SRC_TYPE='PER' and DST_TYPE='PER_ATTR' ;

	CALL _SYS_AFL.PAL_LINK_PREDICT(:datatable, :param_table, :res);

	--resolve node id with meanigful values
	pred_links = select n.NODE_ID, n.VAL as VAL1 , n2.VAL as VAL2, n2.NODE_ID as NODE2, SCORE
	from :res r1
	  inner join V_NODES n on (n.NODE_ID = r1.NODE1)
	  inner join V_NODES n2 on (n2.NODE_ID = r1.NODE2)
	  left outer join :datatable r2 on (r1.NODE1=r2.NODE1 and r1.NODE2=r2.NODE2 and r2.NODE2 is null)
	--where n.val like '%remi%' or n2.val like'%remi%'
	-- r1.NODE1 like 'PER-%' or r2.NODE2 like 'PER_ATTR%'
	 -- and r2.NODE2 like 'PER_ATTR%'
	  -- and
	  --and n.val like '%remi%'
	;

	select * from :pred_links where SCORE>3.5 order by score desc;

end;

call POLER.LINK_PREDICTION() ;
