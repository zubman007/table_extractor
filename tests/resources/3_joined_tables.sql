create table tabschema.new_table as
select *
from tabschema.table1 t1
join tabschema.table2 t2 on t1.c1 = t2.c1
join tabschema.table3 t2 on t2.c1 = t3.c1
;