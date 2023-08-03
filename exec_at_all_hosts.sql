CREATE OR REPLACE PROCEDURE public.exec_at_all_hosts(IN qry text)
 LANGUAGE plpgsql
AS $procedure$
declare
 r text;
 hosts text[]=array['host=localhost port=5432 password=root dbname=work','host=localhost port=45432 password=root','host=localhost port=45433 password=root','host=localhost port=45434 password=root'];
 begin
     foreach r in array hosts loop
         raise notice '%', r;
         perform dblink.dblink_exec(r, qry);
     end loop;
 end;
$procedure$
;

