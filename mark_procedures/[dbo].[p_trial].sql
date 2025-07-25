USE [product]
GO
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

ALTER procedure [dbo].[p_adk_trial]
as
begin

  ---Просмотр результатов загрузки карточек за предыдущий период
  ---апдейт полученного айди в результате загрузки в ПП
  update tco
     set tco.pp_id = l.Response, tco.load_to_crm_date = cast(l.Date as date)
    from produc[ExportData].dbo.[ProspectSalesComment]rialOrgs tco
    join [ExportData].dbo.Load                    l on l.ObjectID = tco.create_pp_id
   where tco.pp_id is null
     and l.Response is not null;


  ---апдейт полученного айди в результате загрузки в КП
  update tco
     set tco.kp_id = ps.OfferId
    from produc[ExportData].dbo.[ProspectSalesComment]rialOrgs tco
    join dbo.ProspectiveSale                    ps on ps.SaleId = tco.pp_id
   where tco.kp_id is null;


  ---апдейт Наличия оплаченного счета по ИНН
  update tco
     set tco.payedbill = 1
    from produc[ExportData].dbo.[ProspectSalesComment]rialOrgs tco
   where exists ( select 1
                    from dbo.bills bc
                   where bc.inn             = tco.inn
                     and bc.BillPayed       > 0
                     and bc.PayDate         >= tco.load_to_crm_date)
     and tco.payedbill is null;



  declare @num_of_created_orgs_all int = ( select count(*)
                                             from produc[ExportData].dbo.[ProspectSalesComment]rialOrgs tco
                                            where tco.load_to_crm_date = cast(dateadd(day, -1, getdate()) as date));


  -----------------------------------------------------------------------------
  ----------------------- Триал. Созданные организации ------------------------
  ------------------------ Метрика ивента регистрации -------------------------
  -----------------------------------------------------------------------------
  -- считывание новых данных из CH
  declare @q as nvarchar(max);
  declare @TrialCreateOrganization nvarchar(max) = N'
				SELECT distinct 
					  visitParamExtractString(Variables, ''inn'') inn
	                , visitParamExtractString(Variables, ''userId'') userId
	                , visitParamExtractString(Variables, ''contactPerson'') userName
	                , visitParamExtractString(Variables, ''phone'') phone
	                , visitParamExtractString(Variables, ''mail'') email
	                , visitParamExtractString(Variables, ''city'') city
	                , ClientDate as Eventdate
	FROM mtk.event 
	WHERE Topic_Key = ''Market''
	  AND Path_Category = ''Service''
	  AND Path_Action = ''Adk.SaveContactsOnCreateTrialOrganizationEvent''
 	and ClientDate = today()-1'; -- для полноты данных считывание всегда будет до "вчера"

  -- запись результатов запроса из CH во временную таблицу
  drop table if exists #d;
  create table #d (inn varchar(12)
                  ,userId uniqueidentifier
                  ,userName varchar(300)
                  ,phone varchar(100)
                  ,email varchar(100)
                  ,city varchar(200)
                  ,EventDate date not null);

  set @q = @TrialCreateOrganization;
  insert into #d
  exec migrations.dbo.CH_query @q;

  -----------------------------------------------------------------------------
  ------------------------ Метрики утм-ивента ---------------------------------
  -----------------------------------------------------------------------------
  -- считывание новых данных из CH
  declare @TrialUtmOrg nvarchar(max) = N'SELECT 
                   visitParamExtractString(Variables, ''userId'')       PortalUserId
                 , visitParamExtractString(Variables, ''utmSource'')    utmSource
                 , visitParamExtractString(Variables, ''utmMedium'')    utmMedium
                 , visitParamExtractString(Variables, ''utmCampaign'')  utmCampaign
                 , visitParamExtractString(Variables, ''utmTerm'')      utmTerm
                 , visitParamExtractString(Variables, ''utmContent'')   utmContent
                 , visitParamExtractString(Variables, ''from'')		    utmFrom
				 , visitParamExtractString(Variables, ''utmOrderPage'') utmOrderPage
				 , visitParamExtractString(Variables, ''utmReferer'')	utmReferer
				 , visitParamExtractString(Variables, ''utmStartPage'') utmStartPage
				 , visitParamExtractString(Variables, ''utmAd'')		utmAd
				 , visitParamExtractString(Variables, ''utmDevice'')	utmDevice
				 , visitParamExtractString(Variables, ''utmRegion'')	utmRegion
				 , visitParamExtractString(Variables, ''utmType'')		utmType
	       , ClientDate as EventDate
	FROM mtk.event 
	WHERE  Path_Category = ''AdkService''
	AND Path_Action = ''Adk.SaveUtmContentEvent''
 	and ClientDate = today()-1'; -- для полноты данных считывание всегда будет до "вчера"

  -- запись результатов запроса из CH во временную таблицу
  drop table if exists #p;
  create table #p (PortalUserId uniqueidentifier
                  ,utmSource varchar(max)
                  ,utmMedium varchar(max)
                  ,utmCampaign varchar(max)
                  ,utmTerm varchar(max)
                  ,utmContent varchar(max)
                  ,[from] varchar(max)
                  ,utmOrderPage varchar(max)
                  ,utmReferer varchar(max)
                  ,utmStartPage varchar(max)
                  ,utmAd varchar(max)
                  ,utmDevice varchar(max)
                  ,utmRegion varchar(max)
                  ,utmType varchar(max)
                  ,ClientDate date not null);

  set @q = @TrialUtmOrg;
  insert into #p
  exec migrations.dbo.CH_query @q;


  drop table if exists #AdkTrialCreateOrganization_new;
  select d.inn
        ,max(userId)    userId
        ,max(userName)  userName
        ,max(phone)     phone
        ,max(email)     email
        ,max(city)      city
        ,max(EventDate) EventDate
    into #AdkTrialCreateOrganization_new
    from #d d
   where not exists ( select 1
                        from produc[ExportData].dbo.[ProspectSalesComment]rialOrgs tco
                       where tco.Inn = d.inn)
   group by inn;


  ---- проверка на существующую поставку или оплаченный счет
  drop table if exists #exists_in_adk;
  select distinct inn
    into #exists_in_adk
    from dbo.RetailPacks rp
    join dbo.Clients      cc on cc.cID = rp.cID
   where rp.is_real = 1
     and getdate() between rp.Since and rp.UpTo
     and rp.tarif_name like 'Отчетность %'
  union
  select distinct inn
    from dbo.bills bc
     and bc.BillPayed       = bc.Summ
     and bc.PayDate         > dateadd(year, -1, getdate());


  ---удаляю таких из банки с триалом (такие попадаются из-за путаницы в сервисе)
  delete tco
    from #AdkTrialCreateOrganization_new tco
   where exists (select 1 from #exists_in_adk m where m.inn = tco.inn);


  drop table if exists #emk;
  select      distinct tco.inn
                      ,first_value(kp.kpp) over (partition by tco.inn order by kp.is_head desc)         kpp
                      ,first_value(kp.region_code) over (partition by tco.inn order by kp.is_head desc) region_code
                      ,first_value(kp.ct) over (partition by tco.inn order by kp.is_head desc)          city
                      ,first_value(kp.addr) over (partition by tco.inn order by kp.is_head desc)        addr
                      ,first_value(kp.short_name) over (partition by tco.inn order by kp.is_head desc)  short_name
                      ,first_value(tco.EventDate) over (partition by tco.inn order by kp.is_head desc)  EventDate
                      ,concat('{"UTM":','"'
                              ,iif(u.utmSource <> '', concat('utm_source=', u.utmSource), u.utmSource)
                              ,iif(u.utmMedium <> '', concat('&utm_medium=', u.utmMedium), u.utmMedium)
                              ,iif(u.utmCampaign <> '', concat('&utm_campaign=', u.utmCampaign), u.utmCampaign)
                              ,iif(u.utmContent <> '', concat('&utm_content=', u.utmContent), u.utmContent)
                              ,iif(u.utmTerm <> '', concat('&utm_term=', u.utmTerm), u.utmTerm)
                              ,iif(u.[from] <> '', concat('&from=', u.[from]), u.[from])
                              ,iif(u.utmOrderPage <> '', concat('&utm_orderPage=', u.utmOrderPage), u.utmOrderPage)
                              ,iif(u.utmReferer <> '', concat('&utm_referer=', u.utmReferer), u.utmReferer)
                              ,iif(u.utmStartPage <> '', concat('&utm_startpage=', u.utmStartPage), u.utmStartPage)
                              ,iif(u.utmAd <> '', concat('&utm_ad=', u.utmAd), u.utmAd)
                              ,iif(u.utmDevice <> '', concat('&utm_device=', u.utmDevice), u.utmDevice)
                              ,iif(u.utmRegion <> '', concat('&utm_region=', u.utmRegion), u.utmRegion)
                              ,iif(u.utmType <> '', concat('&utm_type=', u.utmType), u.utmType)
                            ,'"','}')   SpecificData
                      ,concat('Клиент создал орзанизацию. Познакомьте клиента с сервисом и выставьте счет.','\n'
                             ,'ФИО: ',tco.userName,'\n'
                             ,'Телефон: ',tco.phone,'\n'
                             ,'Электронная почта: ',tco.email,'\n'
                             ,'Дата регистрации: ',tco.EventDate,'\n'
                          )  as comment
                      ,null    as load_to_crm_date
                      ,null    as pp_id
                      ,null    as kp_id
                      ,identity(int, 1, 1)  rowid
    into      #emk
    from      #AdkTrialCreateOrganization_new tco
    left join #p                              u on u.PortalUserId = tco.userId
    join      dbo.fik          kp on kp.inn        = tco.inn
                                                and kp.inn is not null;


  alter table #emk add clientId uniqueidentifier;
  alter table #emk add create_pp_id uniqueidentifier;

  update #emk
     set clientId = newid(), create_pp_id = newid();


  --- Проверка на существующую карточку в ПП
  drop table if exists #existing_pp;
  select distinct e.comment
                 ,e.create_pp_id
                 ,first_value(ps.Id) over (partition by ps.OrganizationInn
                                               order by ps.Temperature, ps.CreateTimeUtc desc) pp_id
    into #existing_pp
    from #emk                                 e
    join dbo.Prospsales  ps on e.inn = ps.OrganizationInn and ps.State in (1,3);


  --- Удаление из подготовленоого списка к загрузке тех организаций, что имеют активную карточку.
  delete from #emk
   where create_pp_id in (select create_pp_id from #existing_pp);


  -----------------------------------------------------------------------------
  ------------------Догрузка комметария в существующую карточку----------------
  -----------------------------------------------------------------------------

  declare @UserId       int = 43 -- для последующих вставок в обменные таблицы
         ,@listNumInt   int
         ,@listNum      varchar(50)
         ,@basename     varchar(200)
         ,@comment_task uniqueidentifier;

  if (select count(*)from #existing_pp) > 0
  begin


    set @basename = 'Рассылка_организаций_Комментарий_existing_activity';
    set @listNumInt = ( select max(left(BriefID, 5))
                          from dbo.Task
                         where BriefID like '%_TrialKMExt');
    set @listNum = replicate('0', 5 - len(@listNumInt + 1)) + cast(@listNumInt + 1 as varchar(5)) + '_TrialKMExt';
    set @comment_task = newid();


    drop table if exists #load3;
    select distinct @comment_task TaskID       -- формируем ИД задачи на загрузку
                   ,@listNum      list_id      -- ИД списка на загрузку
                   ,@basename     basename     -- Название базы
                   ,l.pp_id       ProspectSale -- реальный ИД карточки в КП (ActivityId из AllOffers)
                   ,l.comment                  -- комментарий для карточки
      into #load3
      from #existing_pp l;


    insert into [ExportData].dbo.Task (ID
                                      ,CRM_ID
                                      ,BriefID
                                      ,ExternalID
                                      ,TaskTypeID
                                      ,UserID
                                      ,Name
                                      ,SC
                                      ,Priority
                                      ,StateID)
    select distinct l.TaskID, 2, l.list_id, null, 8, @UserId, l.basename, l.sc_code, 0, 0
      from #load3 l;

    insert into [ExportData].dbo.[ProspectSalesComment] (TaskID, ProspectiveSaleID, AuthorComment, Text)
    select l.TaskID, l.ProspectiveSaleId, l.comment
      from #load3 l;

    -- ЗАГРУЗКА В CRM
    update [ExportData].dbo.Task
       set StateID = 1, Priority = 50
     where ID = (select distinct TaskID from #load3);

  end;


  --выгрузка карточек в КП по загруженным данным
  declare @kp_cards table (cardId varchar(150));

  insert into @kp_cards
  select concat('https://sales/activities/', allo.ActivityId)
    from produc[ExportData].dbo.[ProspectSalesComment]rialOrgs tco
    join dbo.ProspectiveSale                    ps on ps.SaleId    = tco.pp_id
    join CRM.dbo.AllOffers                          allo on ps.OfferId = allo.OfferId
   where load_to_crm_date = cast(dateadd(day, -1, getdate()) as date);


  if (select count(*)from #emk) = 0
  begin


    declare @tableHTML_1 nvarchar(max);
    set @tableHTML_1 = N'<H4>Загрузки по триалу</H4>'
                       + N'<tr><th>Всего созданных организаций за вчера: </th></tr>'
                       + isnull(cast(@num_of_created_orgs_all as nvarchar(max)), 'нет') + N'<br>'
                       + N'<tr><th>Карточки орг созданные в КП за вчера: </th></tr>' + N'<br>'
                       + isnull((select string_agg(cardId, ' <br> ')from @kp_cards), 'не было карточек') + N'<br>'
                       + N'<tr><th>Карточек отправленных на загрузку в ПП сегодня:  Новых созданных организаций по Триалу нет</th></tr>';
    exec msdb.dbo.send_dbmail @profile_name = 'mail profile'
                              ,@subject = 'Информация по загрузке триала'
                              ,@body = @tableHTML_1
                              ,@body_format = 'HTML';

    raiserror('Новых созданных организаций по Триалу нет', 16, 1);
    return;
  end;

