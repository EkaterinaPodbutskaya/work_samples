USE [product]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

ALTER procedure [mark].[p_revenue]
as
begin

  --собираем все новые счета и строки по проекту
  drop table if exists #w;
  select Num, PriceSetName, Cost
    into #w
    from Caches.dbo.c_bill_content cbc
     and PriceSetName    != 'Тест-драйв 7 дней'
     and exists ( select 1
                    from DWS.dbo.cPayment_Content pc
                    join DWS.dbo.cPayments        p on p.bID = cbc.bID
                                                   and p.pID = pc.pID
                   where pc.bcID = cbc.bcID
                     and p.PayDate between '2020-01-01' and getdate())
	and num not in (SELECT num from product.mark.all_revenue);


  update #w
     set PriceSetName = 'Внедрение'
   where PriceSetName like '%стоимость за час%'
			or PriceSetName like '%стоимость за 1 час%'
      or PriceSetName like 'Внедрение%'
      or PriceSetName like 'Инструктаж%'
      or PriceSetName like 'Консультации%'
      or PriceSetName like 'Доработка%';

	update #w
     set PriceSetName = 'Сопровождение'
		where PriceSetName like 'Сопровождение%';

		
	update #w
     set PriceSetName = 'Модуль Оборудование'
		where PriceSetName like 'Модуль Оборудование тариф%';


--SELECT distinct PriceSetName FROM #w  ORDER BY PriceSetName --для проверки что ничего лишнего нет
--чит на случай если что-то не попало
		insert into #w (Num, PriceSetName, Cost)
values ('1', 'API-лицензия'				, '0.00')
      ,('1', 'Базовый'					, '0.00')
      ,('1', 'Внедрение'				, '0.00')
      ,('1', 'Допы'						, '0.00')
      ,('1', 'Коннектор'				, '0.00')
      ,('1', 'Модуль Интеграция'		, '0.00')
	  ,('1', 'Модуль Оборудование'		, '0.00')
      ,('1', 'Пакет'					, '0.00')
	  ,('1', 'Сопровождение'			, '0.00');

		
if ((select count(distinct  PriceSetName)
    from #w
		where PriceSetName not in ('API-лицензия'
								  ,'Базовый'
								  ,'Внедрение'
								  ,'Допы'
								  ,'Коннектор'
								  ,'Модуль Интеграция'
								  ,'Модуль Оборудование'
								  ,'Пакет'
								  ,'Сопровождение'	)) > 0)
begin
  		raiserror('Каких-то полей нехватает', 16, 1);
  		return;
end;


  -- разворачиваем результаты динамическим пивотом во времянку
  drop table if exists #Action;
  create table #Action (num varchar(12)
                       ,[API-лицензия] money
                       ,[Базовый] money
                       ,[Внедрение] money
                       ,[Допы] money
                       ,[Коннектор] money
                       ,[Модуль Интеграция] money
					   ,[Модуль Оборудование] money
                       ,[Пакет] money
					   ,[Сопровождение] money);
  insert into #Action
  exec product.dbo.p_Dynamic_Pivot @TableSRC = '#W'             --Таблица источник (Представление)
                                  ,@ColumnName = 'PriceSetName' --Столбец, содержащий значения для столбцов в PIVOT
                                  ,@Field = 'cost'              --Столбец, над которым проводить агрегацию
                                  ,@FieldRows = 'Num'           --Столбец для группировки по строкам
                                  ,@FunctionType = 'SUM';       --Агрегатная функция, по умолчанию SUM

  update #d_p
     set PriceSetName = 'доставка и бамперы'
   where PriceSetName like 'Бампер защитный%'
      or PriceSetName like 'Доставка%';


--чит на случай если что-то не попало
		insert into #d_p (Num, PriceSetName, Cost)
values ('1', 'Железо'			 , '0.00'),
       ('1', 'допы др проектов'  , '0.00'),
       ('1', 'доставка и бамперы', '0.00');


if ((select count(distinct PriceSetName)
		from #d_p
		where PriceSetName not in ('Железо',
								   'допы др проектов',
								   'доставка и бамперы')) > 0)
begin
  		raiserror('Каких-то полей нехватает', 16, 1);
  		return;
end;

  --разворачиваем результаты пивотом во времянку
  drop table if exists #Action_2;
  create table #Action_2  (num varchar(12)
						 ,[допы др проектов] money
                         ,[доставка и бамперы] money
						 ,[Железо] money);
  insert into #Action_2
  exec product.dbo.p_Dynamic_Pivot @TableSRC = '#d_p'           --Таблица источник (Представление)
                                  ,@ColumnName = 'PriceSetName' --Столбец, содержащий значения для столбцов в PIVOT
                                  ,@Field = 'cost'              --Столбец, над которым проводить агрегацию
                                  ,@FieldRows = 'Num'           --Столбец для группировки по строкам
                                  ,@FunctionType = 'SUM';       --Агрегатная функция, по умолчанию SUM


	delete from #Action_2 where num='1' 
	delete from #Action		where num='1'

  --полная развертка счета с учетом других проектов
  drop table if exists #got;
  select      t.*
             ,isnull(a.[Железо], '')             [Железо]
             ,isnull(a.[доставка и бамперы], '') [доставка и бамперы]
             ,isnull(a.[допы др проектов], '')   [допы др проектов]
    into      #got
    from      #Action   t
    left join #Action_2 a on a.num = t.num;


  --температура предложения из продаж
  drop table if exists #base_info;
  select      distinct t.num, isnull(string_agg(s.ExternalId, '; '), '') extr
    into      #base_info
    from      #got                              t
    left join CRM_DB.dbo.OfferLink l on l.BillNumber = t.num
    left join CRM_DB.dbo.AllOffer  s on l.OfferId    = s.OfferId
          and s.ExternalId is not null
   group by t.num;


  --потенциальная роль по сведениям о карточке
  drop table if exists #inncom;
  select distinct allo.inn,  case
  								when left(a.Description, charindex('Кто клиенты:',a.Description) +1) like'%производ%' then 'Производитель' 
  								when left(a.Description, charindex('Кто клиенты:',a.Description) +1) like'%опт%'	  then 'Оптовик'
  								when left(a.Description, charindex('Кто клиенты:',a.Description) +1) like'%розн%'	  then 'Розница'
  								when left(a.Description, charindex('Кто клиенты:',a.Description) +1) like'%импорт%'	  then 'Импорт'
  						    end role_mk
   into #inncom
  	from  CRM_DB.dbo.AllOffer   Allo 
  	join [CRM_DB].[dbo].[Active] a on a.id=allo.ActivityId
  	where (left(a.Description, charindex('Кто клиенты:',a.Description) +1) like'%производ%' 			
  		or left(a.Description, charindex('Кто клиенты:',a.Description) +1) like'%опт%'
  		or left(a.Description, charindex('Кто клиенты:',a.Description) +1) like'%розн%'	   
  		or left(a.Description, charindex('Кто клиенты:',a.Description) +1) like'%импорт%')
  		and inn in (select inn from product.mark.all_revenue union select inn from #p);


 --обновление для сегмента если мы его узнали
update m
   set m.segment = p.segment
  from product.mark.all_revenue      m
  join product.mark.segmentations p on p.inn     = m.inn
       and p.segment != m.segment
       and p.segment != 'нераспределен';


	--возвратные 
	delete from mark.all_revenue where num in (select num
												  from Cache.dbo.bill_contents 
												  where num in (select num from mark.all_revenue )
												  GROUP BY Num
												  having sum(Payed)=0)

--частичный возврат
delete from mark.all_revenue where num in (select c.Num
											  from Cache.dbo.bill_contents c
											  join mark.all_revenue       s on s.num = c.Num
											  group by c.Num
											  having round(sum(c.Payed),0))

end;

