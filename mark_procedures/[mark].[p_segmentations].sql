SET QUOTED_IDENTIFIER ON
SET ANSI_NULLS ON
GO

ALTER procedure [mark].[p_segmentation]
as
begin

  --пользователи из сервиса с известной товарной группой
  declare @q nvarchar(max) = N'SELECT distinct visitParamExtractString(CustomVariables, ''organizationInn'') organizationInn
								              ,visitParamExtractString(CustomVariables, ''productGroup'') productGroup
							   FROM metrika.tracker_log
							   AND PortalUserId !=''00000000-0000-0000-0000-000000000000''
							   AND organizationInn !=''''
							   AND productGroup !=''''';
							   
  -- запись результатов запроса из CH во временную таблицу (приоритет 1)
  drop table if exists #use;
  create table #use (organizationInn varchar(12)
                    ,productGroup varchar(30));
  insert into #use
  exec migrat.dbo.CH_query @q;


  --проверка на тг которые уже есть в сервисе но еще нет в справочнике
  declare @new_tg varchar(max);

  if ( select count(distinct u.productGroup)
         from #use u
        where not exists ( select *
                             from product.mark.catalog_tags_id i
                            where u.productGroup = i.tags)) > 0
  begin

    drop table if exists #new_tg;
    with cte
      as (select distinct u.productGroup
            from #use u
           where not exists ( select *
                                from product.mark.catalog_tags_id i
                               where u.productGroup = i.tags))
    select string_agg(cte.productGroup, '; ') tg
      into #new_tg
      from cte;

    set @new_tg = (select * from #new_tg);


    declare @tableHTML nvarchar(max);
    set @tableHTML = N'<table border="1">'
                     + N'<tr><th>Новые тг довнести в справочник (product.mark.catalog_tags_id): </th></tr>'
                     + @new_tg + N'</table>';
    exec msdb.dbo.sp_send_dbmail @profile_name = 'mail profile'
                                ,@recipients = '...'
                                ,@subject = 'Новые сегменты'
                                ,@body = @tableHTML
                                ,@reply_to = '...'
                                ,@body_format = 'HTML';
  end;


  --несколько тг в сервисе тоже может быть
  drop table if exists #use_group;
  select organizationInn as inn, string_agg(i.segment, ', ') as pg
    into #use_group
    from #use                                u
    join product.mark.catalog_tags_id i on u.productGroup = i.tags
   group by organizationInn;


  --так как тг из сервиса это наш приоритет сразу тех кого нашли можем обновить
  update si
     set si.segment = g.pg, si.inn = g.inn
    from product.mark.segmentation si
    join #use_group g on g.inn      = si.inn
         and (si.segment != g.pg or si.segment is null);


  --инн-подключений, дата и ответственный менеджер
  drop table if exists #cached;
  select		distinct	inn
                 ,min(p.PayDate)over(partition by inn ORDER BY p.PayDate)  PayDate
                 ,first_value(c.manager_name)over(partition by inn ORDER BY p.PayDate) manager_name
    into #cached
    from Cach.dbo.bill_contents c
		join DWS.dbo.Payments_Content  as pc on pc.bcID = c.bcID
		join DWS.dbo.Payments         as p on pc.pID   = p.pID
   where Payed     > 1
     and inn not in (select distinct inn from product.mark.segmentation)
		 and PriceSetName  like '%Базовый%'
		 GROUP by c.manager_name, c.inn, p.PayDate;


  --сегмент определяется по сведениям о карточке (если они есть) (приоритет 3)
	 drop table if exists #inncom_1;
	 select distinct allo.inn, i.segment
	  into #inncom_1
	 	from  CRM_DB.dbo.AllOffer               Allo
		join [CRM_DB].[dbo].[Active] a on a.id=allo.ActivityId
		join product.mark.catalog_tags_id i on  (a.Description like '%#'+ i.tags +'%'  
											  or a.Description like '% ' + i.tags + '%'
											  or a.Description like '' + i.tags + '%')
		where Allo.inn in (select inn
		                    from #cached
		                   union
		                   select inn
		                    from product.mark.segmentation
		                    where segment = 'нераспределен' or segment is null)
		 and  Allo.Inn not in (select inn from #use_group )
	

drop table if exists #inncom_2;
select Inn, string_agg(i.segment, ', ') as segment
  into #inncom_2
  from #inncom_1 i
 group by Inn;


update si
   set si.segment = g.segment, si.inn = g.Inn
  from product.mark.segmentation si
  join #inncom_2   g on g.Inn      = si.inn
                     and (si.segment != g.segment or si.segment is null);


  --Все новые клиенты с определившимся сегментом
  drop table if exists #id_tag;
  select inn, pg as segment
    into #id_tag  --из сервиса
    from #use_group u
		where exists(select 1 from #cached d where d.inn=u.inn)
  union
  select Inn, segment 
    from #inncom_2  --из коммента
		where exists(select 1 from #cached d where d.inn=u.inn);


  --складываем все сюда
  drop table if exists #t;
  select      distinct datepart(week, PayDate)                                as week_num
                      ,coalesce(string_agg(L.segment, ', '), 'нераспределен') segment
                      ,D.inn
                      ,D.manager_name
                      ,D.PayDate
    into      #t
    from      #cached D
    left join #id_tag L on D.inn = L.inn
   group by datepart(week, PayDate), D.inn, D.manager_name, D.PayDate
   order by PayDate;


  --добавляем новенькие инн в таблицу, указываем тип для построения графиков
  insert into product.mark.segmentation (week_num, segment, inn, manager_name, PayDate, tip)
  select distinct t.week_num
        ,t.segment
        ,t.inn
        ,t.manager_name
        ,t.PayDate
        ,case
           when t.segment like '%,%' then 'мультиотрасль'
           when t.segment = 'нераспределен' then 'неизвестно'
           else t.segment
         end tip
    from #t t
		where inn not in (select inn from product.mark.segmentation);


end;
GO

