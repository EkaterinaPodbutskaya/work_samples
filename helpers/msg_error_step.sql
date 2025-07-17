--отлов ошибок на шагах
--=============================================================================

begin try
  declare @result int;
  execute @result = [product].[p_corp_report]; -- процедура
end try
begin catch
  declare @tableHTML nvarchar(max);
  set @tableHTML = N'<h4>Процедура [product].[p_corp_report]</h4>' + N'<br>' --заголовок внутри письма
                   + N'<tr><th>Ошибка </tr></th>' + convert(varchar, error_number()) + N', Line '
                   + convert(varchar, error_line()) + N' ' + error_message();
  exec msdb.dbo.sp_send_dbmail @profile_name = 'mail profile'
                              ,@recipients = 'podbutskaya@skbkontur.ru'
                              ,@subject = 'Ошибка джоб #product p_corp_report шаг 13' --тема письма
                              ,@body = @tableHTML
                              ,@body_format = 'HTML';
end catch;
