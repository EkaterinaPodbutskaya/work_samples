--пример с рассылкой в excel 

declare @cols as nvarchar(max) = N'';
declare @query as nvarchar(max) = N'';

select  @cols = @cols + quotename(dt) + N','
from    ( select  distinct dt
          from    ofd.test_table
          where   dt is not null) as tmp;
select  @cols = substring(@cols, 0, len(@cols));  

set @query
  = N'SELECT *  from 
(select [ƒата окончани€ поставки],dt,divide from [ofd].test_table) src
pivot (max(divide) for dt in (' + @cols + N')) piv';

exec (@query);

select substring(N''+ quotename(dt) + N'', 0, len(N''+ quotename(dt) + N'')) 
from    ( select  distinct dt
          from    ofd.test_table
          where   dt is not null) as tmp;


 --https://learn.microsoft.com/ru-ru/sql/t-sql/language-elements/variables-transact-sql?view=sql-server-ver16
 --https://stackoverflow.com/questions/55354932/sql-calculate-column-with-dynamic-pivot-queries
 declare @int as int ='5'
 --можно присвоить значение переменной конструкцией set или select
 select @int = '27'
 select @int

----–ассылка писем
declare @d as varchar(20) = cast(cast(getdate() - 1 as date) as varchar(10));

declare @file as varchar(1024) = '\\dwh\Buffer\' + @d + '.xlsx';
declare @xls_q as varchar(1024) = 'select * from [ofd].test_table_2	';
exec InstanceMaintenanceDB..pc_to_excel @xls_q, @file;

execute msdb.dbo.sp_send_dbmail @profile_name = 'mail profile',
                                @recipients = '...@kontur.ru',
                                @body = '—ообщение было создано автоматически',
                                @file_attachments = @file;
