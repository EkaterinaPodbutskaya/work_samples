SET QUOTED_IDENTIFIER ON
SET ANSI_NULLS ON
GO

ALTER procedure [marking].[p_prolong]
as
begin

--все поставки 
drop table if exists #t;
select distinct cID, agentId, bID, tarif_name, Since, UpTo
  into #t
  from dbo.RetailPack
   and is_real     = 1;

   
--доп инфа 
drop table if exists #fas;
select distinct c.inn, isnull(c.kpp, '') kpp, b.Num, t.tarif_name, t.Since, t.UpTo, b.BDate, 
				case
					when t.tarif_name like 'Базовый%' then 'Базовый'
					when t.tarif_name like 'Модуль %' then 'Модуль'
				end tar
  into #fas
  from #t              t
  join dbo.Clients c on c.cID = t.cID
  join dbo.Bill  b on b.bID = t.bID;

		

--выстраивание последовательности поставок
drop table if exists #new_prolong;
with cte
  as (select *, row_number() over (partition by inn, tar order by Since) s
        from #fas)
select     c.inn,
           c.kpp,
           c.Num,
           c.tarif_name,
           c.Since,
           c.UpTo ,
           iif(t.s is not null and datediff(dd, c.UpTo, t.Since) < 180, 1, 0) prolong,
           datediff(dd, c.UpTo, t.Since)                                      defer,
           t.Num                                                              [продляющий счет],
		   t.tarif_name														  [тариф продления],
           t.BDate                                                            [дата выставления счета на продление],
           t.Since                                                            [дата начала поставки-продления],
		   c.tar
  into      #new_prolong
  from      cte c
  left join cte t on t.inn = c.inn
                 and t.s   = c.s + 1 and t.tar like c.tar


drop table if exists #new_5;
select inn,
       kpp,
       Num,
       tarif_name,
       Since ,
       UpTo	 ,
       prolong,
       defer,
       iif(prolong = 0, null, [продляющий счет])                     [продляющий счет],
	   iif([тариф продления] is null, '', [тариф продления])		 [тариф продления],
       iif(prolong = 0, null, [дата выставления счета на продление]) [дата выставления счета на продление],
       iif(prolong = 0, null, [дата начала поставки-продления])      [дата начала поставки-продления],
	   tar
  into #new_5
  from #new_prolong
 order by defer desc;


--где-то в отчете по выручке ошибка в определении сегмента счета
--костыль исправляет недочет
update product.mark.all_revenue
   set revenueT = 'продление'
 where num in ( select r.num
                  from product.mark.all_revenue r
                  join #new_5              n on n.[продляющий счет] = r.num
                 where r.revenueT = 'подключение');


--данные по отказу от предолжения
drop table if exists  #new_6;
select distinct n.inn, 
                first_value(Allo.RejectReason) over (partition by n.inn order by Allo.rejectdate desc)RejectReason
into #new_6
  from #new_5               n
  join CRM_DB.dbo.AllOffer Allo on Allo.Inn   = n.inn
                                and CrmProduct = '...'
                                and Allo.RejectReason is not null
 where prolong = 0;


 --итоговая
drop table if exists #new_8;
select distinct year(dateadd(dd, 1, UpTo)) [год продления], month(dateadd(dd, 1, UpTo)) [месяц продления], n.*, ifi.stts, w.RejectReason, getdate() date_load
into #new_8
  from #new_5 n
  left join #new_6 w on w.inn=n.inn
  left join dbo.fik ifi on ifi.inn=n.inn
 order by year(dateadd(dd, 1, UpTo)), month(dateadd(dd, 1, UpTo));


--потенциальная роль маркировки по сведениям о карточке
drop table if exists #inncom;
select distinct allo.inn,  case
								when left(a.Description, charindex('Кто клиенты:',a.Description) +1) like'%производ%' then 'Производитель' 
								when left(a.Description, charindex('Кто клиенты:',a.Description) +1) like'%опт%'	  then 'Оптовик'
								when left(a.Description, charindex('Кто клиенты:',a.Description) +1) like'%розн%'	  then 'Розница'
								when left(a.Description, charindex('Кто клиенты:',a.Description) +1) like'%импорт%'	  then 'Импорт'
						    end role_mkvk
 into #inncom
	from  CRM_DB.dbo.AllOffer               Allo 
	join [CRM_DB].[dbo].[Active] a on a.id=allo.ActivityId
	where (left(a.Description, charindex('Кто клиенты:',a.Description) +1) like'%производ%' 			
		or left(a.Description, charindex('Кто клиенты:',a.Description) +1) like'%опт%'
		or left(a.Description, charindex('Кто клиенты:',a.Description) +1) like'%розн%'	   
		or left(a.Description, charindex('Кто клиенты:',a.Description) +1) like'%импорт%')
		and inn in (select inn from #new_8);


drop table if exists #role_s;
select inn, string_agg(role_mkvk, '; ') role_mkvk
  into #role_s
  from #inncom
 group by inn;


 --деньги по строкам
 drop table if exists #summa
 select num, cbc.TariffName, sum(Payed) Payed 
 into #summa
 from dbo.bill_contents cbc
 where cbc.bID in (select bID from #t)
 GROUP BY cbc.Num, cbc.TariffName


-- вывод для отчета
begin transaction;
truncate table product.mark.prolong;
insert into product.mark.prolong( [год продления]
												,[месяц продления]
												,[инн	]
												,[кпп	]
												,[потенциальная роль ]
												,[тг]
												,[счет	]
												,[сумма за тариф] 
												,[СЦ	]
												,[тариф поставки]
												,[начало поставки	]
												,[окончание поставки]	
												,[признак продления	]
												,[разница между основной и продляющей поставкой в днях	]
												,[тариф продления]
												,[продляющий счет]
												,[дата выставления счета на продление]
												,[разница дат выставления и оплаты счета на продление]
												,[дата начала поставки-продления]
												,[статус по фокусу	]
												,[причина отказа (если была)	]
												,date_load)
select distinct t.[год продления],
                t.[месяц продления],
                t.inn			,
                t.kpp			,
				isnull(a.role_mkvk, '') [потенциальная роль МК],
				iif(s.segment = 'нераспределен' or s.segment is null, isnull(w.fr, ''), s.segment) tg,
                t.Num			,
				m.Payed,
				cbc.code		,
                t.tarif_name	,
                t.Since			,
                t.UpTo			,
                t.prolong		,
                t.defer			,
				[тариф продления],
                t.[продляющий счет],
                t.[дата выставления счета на продление],
				datediff(dd, t.[дата выставления счета на продление], r.PayDate)[разница дат выставления и оплаты счета на продление],
                t.[дата начала поставки-продления],
                t.stts          ,
                t.RejectReason  ,
                t.date_load	
	from  #new_8 t
	join dbo.bill_contents cbc on cbc.num=t.num
	left join  product.mark.all_revenue r on r.num=t.[продляющий счет]
	left join product.mark.segmentations s on s.inn  = t.inn
	left join product.mark.all_categories   w on w.inn  = t.inn
	left join #role_s a on a.inn=t.inn
	left join #sums m on m.num=t.num and t.tar = m.TariffName
	ORDER BY t.inn, t.tarif_name;

	commit transaction;

end;
GO

