DROP TABLE IF EXISTS #t
create TABLE #t(
	   inn		 VARCHAR(12)
	 , модель	 VARCHAR(12)
	 , ид_кассы  VARCHAR(12)
	 , дата_рег  date
	 , дата_фиск date)

	insert into #t (inn, модель, ид_кассы, дата_рег, дата_фиск)
	values ('1', 'модель1', '2', '2017-06-01', '2018-06-01'),
				 ('1', 'модель1', '1', '2017-01-01', '2018-01-01');


with cte_1 as (select модель, ид_кассы, дата_рег
                 from #t
               union all
               select  t.модель, e.ид_кассы, dateadd(month, 1, e.дата_рег) дата_рег
                 from #t    t
                 join cte_1 e on e.дата_рег < t.дата_фиск
                             and t.ид_кассы = e.ид_кассы )
select модель, count(cte_1.ид_кассы), cte_1.дата_рег
  from cte_1
 group by модель, cte_1.дата_рег;

