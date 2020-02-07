drop SCHEMA POLER cascade;

CREATE SCHEMA POLER;

set SCHEMA POLER;

CREATE TABLE PERSON (
    person_id NVARCHAR(20) not null,
    fullname  NVARCHAR(255),
    firstname NVARCHAR(128),
    lastname  NVARCHAR(128),
    gender    NCHAR(1),
    dob       date,
    -- place_of_birth NVARCHAR(255),
    src_system NVARCHAR(128)
);

create unique index PK_PERSON on PERSON(person_id);


--Attributes related to a person
-- nationalities, pictures, id in various systems
-- distinguishing_marks, weight
CREATE TABLE PERSON_ATTR (
    entity_id      NVARCHAR(20) not null,
    attr_id        NVARCHAR(20),
    attr_parent_id NVARCHAR(20),
    attr_name      NVARCHAR(128),
    attr_full_name NVARCHAR(255),
    attr_type      NVARCHAR(255),
    attr_data_type NVARCHAR(10),
    attr_string_value NVARCHAR(512) null,
    attr_num_value double null,
    attr_date      seconddate null,
    src_system     NVARCHAR(128)
) ;
create unique index PK_PERSON_ATTR on PERSON_ATTR(attr_id);
create index IDX_PERSON_ATTR on PERSON_ATTR(entity_id);


CREATE TABLE OBJECT(
    obj_id     NVARCHAR(20) not null,
    category   NVARCHAR(255),
    obj_label  NVARCHAR(255),
    src_system NVARCHAR(128)
);
create unique index PK_OBJECT on OBJECT(obj_id);


CREATE TABLE OBJECT_ATTR (
    entity_id      NVARCHAR(20) not null,
    attr_id        NVARCHAR(20),
    attr_parent_id NVARCHAR(20),
    attr_name      NVARCHAR(128),
    attr_full_name NVARCHAR(255),
    attr_type      NVARCHAR(255),
    attr_data_type NVARCHAR(10),
    attr_string_value NVARCHAR(2000) null,
    attr_num_value double null,
    attr_date      seconddate null,
    src_system     NVARCHAR(128)
) ;
create unique index PK_OBJECT_ATTR on OBJECT_ATTR(attr_id);
create index IDX_OBJECT_ATTR2 on OBJECT_ATTR(attr_parent_id);
create index IDX_OBJECT_ATTR on OBJECT_ATTR(entity_id);


CREATE TABLE LOCATION (
    loc_id nvarchar(20) not null,
    loc_type nvarchar(255),
    loc_label nvarchar(255),
    loc_data_type nvarchar(64),
    loc_pt st_point(1000004326) validation full ,
    loc_geom st_geometry(1000004326) validation full,
    loc_geom_center st_point(1000004326) validation full,
    src_system nvarchar(128)
) ;
create unique index PK_LOCATION on LOCATION(loc_id);

create sequence SEQ_OBJ START with 1 ;
create sequence SEQ_OBJ_ATTR START with 1 ;
create sequence SEQ_PER START with 1 ;
create sequence SEQ_PER_ATTR START with 1 ;
create sequence SEQ_LOC START with 1 ;
create sequence SEQ_LOC_ATTR START with 1 ;
create sequence SEQ_REL START with 1 ;
create sequence SEQ_REL_ATTR START with 1 ;

CREATE TABLE RELATIONSHIP (
    rel_id    bigint primary key,
    src_type  nvarchar(10),
    dst_type  nvarchar(10),
    src_id    nvarchar(20) not null,
    dst_id    nvarchar(20) not null,
    rel_type  nvarchar(20),
    src_system nvarchar(128)
) ;

CREATE INDEX IDX_REL on RELATIONSHIP(src_id) ;
CREATE INDEX IDX2_REL on RELATIONSHIP(dst_id);

CREATE or replace VIEW V_NODES as
SELECT attr_id as node_id,
	   attr_name as short_label,
	   attr_full_name as label,
	   attr_name||':'||coalesce(attr_string_value,to_nvarchar(attr_num_value)) as val
from OBJECT_ATTR
union all
SELECT attr_id as node_id,
	   attr_name as short_label,
	   attr_full_name as label,
	   attr_name||':'||coalesce(attr_string_value,to_nvarchar(attr_num_value)) as val
from PERSON_ATTR
union all
select person_id,
    fullname,
    fullname||' ('||gender||')',
    fullname
from PERSON
union all
select obj_id,
    category ,
    obj_label,
	obj_label
from OBJECT	;

CREATE GRAPH WORKSPACE INTERPOL
	EDGE TABLE RELATIONSHIP
	SOURCE COLUMN src_id
	TARGET COLUMN dst_id
	KEY COLUMN rel_id
	VERTEX TABLE V_NODES
	KEY COLUMN node_id ;


grant select on schema POLER to public;

--the graph explorer in the webide doesn't like graphs built on top of views so we materialize the nodes:
-- and create a second graph on physical table
create table POLER.MATERIALIZED_NODE as (select * from POLER.V_NODES) with data;
alter table POLER.MATERIALIZED_NODE add constraint node_pk primary key(node_id);


--view to exclude relasionship between individuals of the exact same height and weight
CREATE or replace VIEW "POLER"."V_REL"  AS SELECT "REL_ID" , "SRC_TYPE" , "DST_TYPE" , "SRC_ID" , "DST_ID" , "REL_TYPE" , "SRC_SYSTEM" from POLER.relationship
where dst_type <> 'PER_ATTR'
   or (DST_ID not in (select ATTR_ID from "POLER"."PERSON_ATTR" where attr_data_type='number' and attr_name not like '%approx'))

CREATE GRAPH WORKSPACE POLER.INTERPOL2
	EDGE TABLE POLER.V_REL
	SOURCE COLUMN src_id
	TARGET COLUMN dst_id
	KEY COLUMN rel_id
	VERTEX TABLE POLER.MATERIALIZED_NODE
	KEY COLUMN node_id ;

  create or replace procedure POLER.POST_LOAD()
  DEFAULT SCHEMA "POLER"
  as
  begin

  	--remove weight=0 and height=0
  	delete from "POLER".PERSON_ATTR
  	where (attr_data_type='number' and attr_num_value=0)
  	  or src_system='POST-LOAD';

  	--remove dangling edges
  	delete from "POLER".relationship
  	  where src_id not in (select node_id from POLER.V_NODES)
  	     or dst_id not in (select node_id from POLER.V_NODES)
  	     or src_system='POST-LOAD';

  	--make categories for numerical person attribute (weight, height)
  	bin_nums=
  		select attr_id,attr_full_name,attr_name,attr_num_value,
  		       --4 equal width bins
  		       BINNING(VALUE => attr_num_value, BIN_COUNT => 4) OVER (partition by attr_full_name order by attr_num_value) AS bin_num
  		from RELATIONSHIP r
  		 inner join PERSON_ATTR a on (r.DST_ID=a.ATTR_ID)
  		where attr_data_type='number'
  	      and attr_num_value>0 ;

  	bins= select 'PER_ATTR-'||SEQ_PER_ATTR.nextval as attr_id,
  	             attr_full_name as attr_full_name_orig,
  	             attr_full_name||'-approx' as attr_full_name,
  	             min(attr_name)||'-approx' as attr_name,
  	             bin_num,
  	             median(attr_num_value) as attr_num_value,
  	             '['||min(attr_num_value)||'-'||max(attr_num_value)||']' as attr_string_value
  	from :bin_nums
  	group by attr_full_name, attr_full_name||'-approx', bin_num;

  	--insert the bins as new person attributes
  	insert into PERSON_ATTR(attr_id, attr_full_name, attr_name, attr_string_value, attr_num_value, entity_id, attr_type, attr_data_type, src_system)
  	select attr_id, attr_full_name, attr_name, attr_string_value,
  	       attr_num_value, '' as entity_id,
  	       'simple' as attr_type,
  	       'number' as attr_data_type,
  	       'POST-LOAD' as src_system
  	from :bins;

  	--create relationships from persons to the new categories
  	--TODO, remove duplicates
  	new_rels = select distinct r.src_type,
  	       r.dst_type,
  	       r.src_id,
  	       b2.attr_id as dst_id,
  	       r.rel_type,
  	       'POST-LOAD' as src_system
  	from relationship r
  	  inner join :bin_nums b1 on (r.dst_id = b1.attr_id)
  	  inner join :bins b2 on (b1.bin_num = b2.bin_num and b1.attr_full_name=b2.attr_full_name_orig);

  	insert into relationship(rel_id, src_type, dst_type, src_id, dst_id, rel_type, src_system)
  	select SEQ_REL.nextval, * from :new_rels;



  	--the graph explorer in the webide doesn't like graphs built on top of views so we materialize the nodes:
  	delete from POLER.MATERIALIZED_NODE;
  	insert into POLER.MATERIALIZED_NODE select * from POLER.V_NODES;

  end;


  create view V_REVERSE_RELATIONSHIPS as (
  	select * from V_REL
  	union all
  	select -1*"REL_ID",
  	"DST_TYPE",
  	"SRC_TYPE",
  	"DST_ID",
  	"SRC_ID",
  	"REL_TYPE",
  	"SRC_SYSTEM" from V_REL
  );

  CREATE GRAPH WORKSPACE INTERPOL_BIDIRECTIONAL
  	EDGE TABLE V_REVERSE_RELATIONSHIPS
  	SOURCE COLUMN src_id
  	TARGET COLUMN dst_id
  	KEY COLUMN rel_id
  	VERTEX TABLE V_NODES
  	KEY COLUMN node_id ;
