//find maximum salary drawn in each deplartment
spark.sql("select dept, max(salary) as maxSalary from employee group by dept").show
spark.sql("select dept,salary from (select dept, salary, row_number() over (partition by dept order by salary desc) as rnk from employee) t where rnk=1").show

// COMMAND ----------

//find top two people drwaing max salary in each department
spark.sql("select id, name, dept,salary from (select id, name, dept, salary, rank() over (partition by dept order by salary desc) as rnk from employee) t where rnk<3").show

// COMMAND ----------

//find employees drwaing more salary than departmental avg salary
spark.sql("select id, name, e.dept , salary, avgsalary from employee e,  (select dept, avg(salary) as avgSalary from employee group by dept) t where e.dept=t.dept and e.salary>avgsalary order by dept").show
spark.sql("select  id, name, dept, salary from (select id, name, dept, salary, avg(salary) over (partition by dept order by dept desc) as avgSalary from employee) t where salary>avgsalary").show

// COMMAND ----------

//find departments drwaing maximum salary
spark.sql("select dept, sum(salary) as totalsalary from employee group by dept").createOrReplaceTempView("sumSal")
spark.sql("select dept, totalsalary  from sumSal order by totalsalary desc limit 1 ").show
//spark.sql("select dept, max(totalsalary) from (select dept, sum(salary) as totalsalary from employee group by dept)  ").show
//spark.sql("select distinct dept, sum(salary) over (partition by dept order by dept desc) as totalsalary from employee").show

// COMMAND ----------

//find department having oldest employees

// COMMAND ----------

//find employees drawing minimum and maximum salary in each department
