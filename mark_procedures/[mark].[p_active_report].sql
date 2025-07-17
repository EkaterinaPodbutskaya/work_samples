SET QUOTED_IDENTIFIER ON
SET ANSI_NULLS ON
GO
 
ALTER procedure [mark].[active_report]
as
begin

  --все текущие поставки и поставки которые еще не начались
  drop table if exists #t;
  select distinct cID, agentId, bID, tarif_name, Since, UpTo, '1' [текущие]
   into #t
    from DWS.dbo.RetailPack
     and getdate() between Since and UpTo
     and is_real     = 1
	union
select distinct cID, agentId, bID, tarif_name, Since, UpTo, '0' [текущие]
  from DWS.dbo.RetailPack
   and Since       > getdate()
   and is_real     = 1


  --дополняем данные
  drop table if exists #test;
  select      distinct c.inn
                      ,isnull(c.kpp, '')    kpp
					  ,ifi.short_name
					  ,ifi.role
					  ,ifi.revenue_lastKnown
                      ,a.code  as hold
                      ,z.st_name
                      ,iif(z.SCSegment = 'Остальные', 'Масс', z.SCSegment) SC
                      ,z.Department
                      ,b.BDate
                      ,b.Summ
                      ,b.Payed
                      ,b.PayDate
                      ,w.Since
                      ,w.UpTo
                      ,w.tarif_name
                      ,ifi.stts
                      ,b.bID
                      ,z.code as sc_m
                      ,c.cID
					  ,[w].[текущие]
  into      #test
    from      #t                             w
    join      DWS.dbo.Clients  c on c.cID     = w.cID 
    join      DWS.dbo.Bill     b on b.bID     = w.bID
    join      DWS.dbo.Agents   z on w.agentId = z.vcuID
    left join dbo.fik          ifi on ifi.inn = c.inn
              and ifi.kpp        = c.kpp
    left join BillyAccounts.dbo.PayerLink bap on bap.PayerId    = w.cID
              and ExpirationDate > getdate()
              and ProductId      = 'Mark'
    left join DWS.dbo.cAgent a on a.code = bap.ServiceCenterCode;


  --обьединяем тарифы поставок
  drop table if exists #w;
  select distinct inn
                 ,kpp
				 ,short_name
				 ,t.role
				 ,revenue_lastKnown
                 ,zakrep
                 ,st_name
                 ,SC
                 ,Department
                 ,BDate
                 ,Summ
                 ,Payed
                 ,PayDate
                 ,Since
                 ,UpTo
                 ,string_agg(tarif_name, '; ') tarif
                 ,stts
                 ,bID
                 ,t.sc_m
    into #w
    from #test t
   group by inn, kpp, short_name, role, revenue_lastKnown, st_name, SC, Department, BDate, Summ, Payed, PayDate, Since, UpTo, stts, bID, t.sc_m;


  --все пользователи которые заходили в сервис
  declare @VisitUsers nvarchar(max) = N'SELECT distinct visitParamExtractString(CustomVariables, ''organizationInn'') organizationInn
													   ,visitParamExtractString(CustomVariables, ''productGroup'') productGroup
									FROM metrika.tracker_log
									WHERE  SiteId  =''79''
									AND EventDate<today( ) and organizationInn !=''''';

  -- запись результатов запроса из CH во временную таблицу
  drop table if exists #visitors;
  create table #visitors (organizationInn varchar(12)
                         ,productGroup varchar(30));
  insert into #visitors
  exec migrat.dbo.CH_query @VisitUsers;


  --пользователи с ключевыми действиями
  declare @ActiveUsers nvarchar(max) = N'SELECT distinct visitParamExtractString(CustomVariables, ''organizationInn'') organizationInn
														,visitParamExtractString(CustomVariables, ''productGroup'') productGroup
														, Category, Action, MAX(EventDate) date
										FROM metrika.tracker_log
										WHERE  SiteId  =''79''
										AND EventDate<today( ) 
										AND Action NOT IN (''Переход в раздел'', ''Отправка в архив'')
										AND Category NOT IN (''Сотрудники'', ''Настройки'')
										and organizationInn !=''''
										GROUP BY CustomVariables, Category, Action';

  -- запись результатов запроса из CH во временную таблицу
  drop table if exists #Action;
  create table #Action (organizationInn varchar(12)
                       ,productGroup varchar(30)
                       ,Category varchar(100)
                       ,Action varchar(100)
                       ,Date date not null);
  insert into #Action
  exec migrat.dbo.CH_query @ActiveUsers;


  --агрегирую категории и экшены в ключевые метрики
  drop table if exists #cert_act;
  select distinct t.organizationInn  as inn
                 ,iif(Action = 'Создание документов', 1, 0) 'на отгрузке создание д-ов'
                 ,iif(Category != 'на отгрузке', 1, 0)   'Остальные'
                 ,t.Date
    into #cert_act
    from #Action   t;


  --объединяем данные
  drop table if exists #com;
  select      distinct w.*
                      ,iif(v.organizationinn is not null, 1, 0) enter
                      ,ac.last_date
    into      #com
    from      #w        w
    left join #visitors v on v.organizationinn   = w.inn
    left join #active   ac on ac.inn = w.inn;


  --справочник исключений для вычитаний сумм
  drop table if exists #spr_del;
  select distinct PriceSetName
    into #spr_del
    from Cache.dbo.bill_contents cbc
   where  (PriceSetName like 'Инструктаж%'
				or PriceSetName like 'Рабочее место%'
				or PriceSetName like 'Внедрение%'
        or PriceSetName like '%стоимость за час%'
        or PriceSetName like '%стоимость за 1 час%'
				or PriceSetName like 'Бампер%'
				or PriceSetName like 'Доставка%'
				or PriceSetName like 'Пакет для обработки%') 
				and bid in (SELECT bid 	FROM #com);
		

  --сумма продления, суммы всего счета вычитаем разовые услуги и железо
  drop table if exists #test_2;
  select distinct cbc.bID, cbc.Summ - sum(Cost) as pure_summ
    into #test_2
    from Cache.dbo.bill_contents cbc
   where PriceSetName in (select * from #spr_del)
     and cbc.bID in (select bID from #t) 
   group by bID, Summ;


  --добавляем данные о других продуктах + сегмент товарной группы
  drop table if exists #rez;
  select      distinct t.*
                      ,i.segment
                      ,iif(m.bID is not null, m.pure_summ, null)   pure_summ
                      ,dateadd(day, 1, t.UpTo)                     Plan_date
    into      #rez
    from      #com                                 t
    left join #test_2                              m on m.bID  = t.bID
    left join product.mark.segmentations i on i.inn  = t.inn
    left join dbo.iproduct            p on p.inn  = t.inn
                                                    and p.kpp  = t.kpp
                                                    and p.have = 1;


  -- убираем задвоения и форматируем дату по тз заказчика
  drop table if exists #s;
  select distinct r.inn
                 ,r.kpp
				 ,short_name
				 ,role
				 ,revenue_lastKnown
                 ,st_name
                 ,SC
                 ,r.Department
                 ,r.BDate
                 ,r.Payed
                 ,format(cast(r.PayDate as date), 'dd.MM.yyyy') as PayDate
                 ,format(cast(r.Since as date), 'dd.MM.yyyy')   as Since
                 ,format(cast(r.UpTo as date), 'dd.MM.yyyy')    as UpTo
                 ,tarif
                 ,enter
                 ,max(last_date)  as last_date
                 ,stts
                 ,segment as tg
                 ,coalesce(r.pure_summ, r.Summ) as Summ_plan
                 ,format(cast(Plan_date as date), 'dd.MM.yyyy') Plan_date
                 ,r.bID
                 ,r.sc_m
                 ,r.Since as Since_o
                 ,r.UpTo  as UpTo_o
    into #s
    from #rez r
   group by r.inn
           ,r.kpp
		   ,short_name
		   ,role
		   ,revenue_lastKnown
           ,st_name
           ,SC
           ,r.Department
           ,r.Summ
           ,r.BDate
           ,r.Payed
           ,r.PayDate
           ,Since
           ,UpTo
           ,tarif
           ,enter
           ,stts
           ,segment
           ,pure_summ
           ,Plan_date
           ,r.bID
           ,r.sc_m;



  --дубли из-за нескольких сц закрепа
  drop table if exists #doobl_sc;
  select inn
    into #doobl_sc
    from #doobles
   group by inn
  having count(distinct hold) > 1;


  --оставляем только сц закрепа который = сц счета
  delete from #doobles
   where inn in (select inn from #doobl_sc)
     and sc_m != hold;

	
  --группируем строку по сумме всех тарифов которые есть на клиенте, инфо оставляем 
  --только для основного тарифа базовый
  drop table if exists #pure_doob;
  select d.inn
        ,d.kpp
		,d.short_name
		,d.role
		,d.revenue_lastKnown
        ,d.st_name
        ,d.SC
        ,d.Department
        ,d.BDate
        ,d.Payed
        ,d.PayDate
        ,d.Since
        ,d.UpTo
        ,string_agg(s.tarif, '; ') tarif
        ,d.enter
        ,d.last_date
        ,d.stts
        ,sum(s.Summ_plan)          summ_plan
        ,d.Plan_date
        ,d.bID
        ,d.sc_m
        ,d.Since_o
        ,d.UpTo_o
    into #pure_doob
    from #doobles d, #doobles s
   where d.tarif like '%базовый%' and 
	 d.inn = s.inn and d.[текущие]=s.[текущие]
   group by d.inn
           ,d.kpp
		   ,d.short_name
		   ,d.role
		   ,d.revenue_lastKnown
           ,d.st_name
           ,d.SC
           ,d.Department
           ,d.BDate
           ,d.Payed
           ,d.PayDate
           ,d.Since
           ,d.UpTo
           ,d.enter
           ,d.last_date
           ,d.stts
           ,d.tg
		   ,d.Plan_date
           ,d.bID
           ,d.sc_m
           ,d.Since_o
           ,d.UpTo_o;


  --обьединяем данные 
  drop table if exists #p;
  select *
    into #p
    from #pure_doob
  union
  select *
    from #s
   where inn not in (select inn from #pure_doob);
	
	
	--попадут только те кто есть и в текущих и в будущих поставках
	DROP TABLE IF EXISTS #p_2
	SELECT p1.inn
          ,iif((p1.summ_plan - p2.summ_plan)<0, '0',  (p1.summ_plan - p2.summ_plan)) sum_plan
          ,isnull(p2.Plan_date, p1.Plan_date) Plan_date
          ,p1.bID
		  ,p2.tarif as tarif_2
		  ,p2.Since_o as Since_2
		  ,p2.Upto_o as Upto_2
	into #p_2
	FROM #p as p1, #p as p2
	where p1.inn=p2.inn

	--для перевода портальников в инн
	declare @inn nvarchar(max) = N'select distinct visitParamExtractString (CustomVariables, ''organizationInn''), PortalUserId
								  from metrika.tracker_log
								  where  SiteId = 79
								  and PortalUserId!=''00000000-0000-0000-0000-000000000000''
								  and visitParamExtractString (CustomVariables, ''organizationInn'')!=''''';
  -- запись результатов запроса из CH во временную таблицу
  drop table if exists #inn;
  create table #inn (inn  varchar(12)
                     ,PortalUserId uniqueidentifier);
  insert into #inn
  exec migration.dbo.CH_query  @inn;



end;
GO

