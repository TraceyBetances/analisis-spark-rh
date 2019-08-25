/* En este análisis se va a partir de información previamente ingestada en HDFS
y de donde se crearon tablas de Hive sobre datos de Recursos Humanos.
Concretamente, se trata de una serie de consultas que el departemneto de
Recursos Humanos solicita y para cuya respuesta se tiene que combinar y
relacionar toda la información disponible. */



object peticiones_RH {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    /**
      * Primera consulta: Se solicita el domicilio de todos los departmentos de la
    empresa.
      * Empezamos extrayendo la información a Spark y guardándola en un DataFrame.
      * Ejecutamos la consulta a través de un Join entre las tablas "locations" y "countries".*/

    val DF_query1 = spark.sql("""SELECT l.location_id,
                                |         l.street_address,
                                |         l.city,
                                |         l.state_province,
                                |         c.country_name,
                                |         c.country_id
                                |FROM locations l
                                |JOIN countries c
                                |     ON l.country_id=c.country_id
                                |""")

    /**
      * Cargamos el resultado de la consulta en formato CSV en la carpeta del proyecto en HDFS para
        su descarga y posterior envío.
      */
    val W_query1 = DF_query1.repartition(1).write.option("header","true").csv("/rrhh/peticiones_departamento_RRHH/domicilios_dep.csv")

/**
  * Segunda consulta: Se solicitan todos los nombres de empleados y sus correspondientes departamentos.
  * Cargamos la información necesaria a Spark.
  */

    val DF_query2 = spark.sql("""SELECT E.last_name,
                                |         E.department_id,
                                |         D.department_name
                                |FROM employees E
                                |JOIN departments D
                                |    ON E.department_id = D.department_id
                                |ORDER BY  department_name
                                |""")

    /**
      * Cargamos el resultado de la consulta en formato CSV en la carpeta del proyecto en HDFS para
        su descarga y posterior envío.
      */

    val W_query2 = DF_query2.repartition(1).write.option("header","true").csv("/rrhh/peticiones_departamento_RRHH/empleados_y_departamentos.csv")

    /**
      * Tercera consulta: Se solicitan los datos de los empleados de Toronto.
      * Cargamos la información necesaria a Spark.
      */

    val DF_query3 = spark.sql("""
                                |SELECT E.last_name,
                                |         E.job_id,
                                |         J.job_title,
                                |         D.department_id,
                                |         D.department_name,
                                |         L.city
                                |FROM employees E
                                |JOIN jobs J
                                |    ON E.job_id = J.job_id
                                |JOIN departments D
                                |    ON E.department_id = D.department_id
                                |JOIN locations L
                                |    ON D.location_id = L.location_id
                                |WHERE city = 'Toronto'
                                |""")

/**
  * Cargamos el resultado de la consulta en formato CSV en la carpeta del proyecto en HDFS para
  * su descarga y posterior envío.
  */
val W_query3 = DF_query3.repartition(1).write.option("header","true").csv("/rrhh/peticiones_departamento_RRHH/empleados_Toronto.csv")
    /**
      * Cuarta consulta: Se solicitan los datos de los empleados y sus respectivos managers.
      * Esta vez se ha realizado un SelfJoin puesto que la información se encontraba en la misma tabla.
      * Cargamos la información necesaria a Spark.
      */

    val DF_query4 = spark.sql("""SELECT W.last_name AS employee,
                                |         W.employee_id AS emp_num,
                                |         M.last_name AS manager,
                                |         M.employee_id AS mgr_num
                                |FROM employees W
                                |JOIN employees M
                                |    ON (W.manager_id = M.employee_id)""")

    /**
      * Cargamos el resultado de la consulta en formato CSV en la carpeta del proyecto en HDFS para
      * su descarga y posterior envío.
      */

    val W_query4 = DF_query4.repartition(1).write.option("header","true").csv("/rrhh/peticiones_departamento_RRHH/empleado_manager.csv")

    /**
      * Quinta consulta: Se solicitan los datos de los empleados y sus respectivos managers incluido King, que es el CEO.
      * Cargamos la información necesaria a Spark.
      */

    val DF_query5 = spark.sql("""SELECT E.last_name AS empleado,
                                |         E.employee_id AS emp_num,
                                |         M.last_name AS mgr,
                                |         M.employee_id AS mgr_num
                                |FROM employees E LEFT OUTER
                                |JOIN employees M
                                |    ON (E.manager_id = M.employee_id)
                                |    ORDER BY emp_num""")

    /**
      * Cargamos el resultado de la consulta en formato CSV en la carpeta del proyecto en HDFS para
      * su descarga y posterior envío.
      */

    val W_query5 = DF_query5.repartition(1).write.option("header","true").csv("/rrhh/peticiones_departamento_RRHH/empleado_manager_CEO.csv")

    /**
      * Sexta consulta: Se solicitan los datos de un empleado y todos los de su departamento.
      * Cargamos la información necesaria a Spark.
      */
    val DF_query6 = spark.sql("""SELECT nombre, apellido, depart_id, nombre_comp, apellido_comp, depart_comp
                                |FROM(
                                |
                                |SELECT E.first_name AS nombre,
                                |         E.last_name AS apellido,
                                |         E.department_id AS depart_id,
                                |         D.first_name AS nombre_comp,
                                |         D.last_name AS apellido_comp,
                                |         D.department_id AS depart_comp
                                |FROM employees E
                                |JOIN employees D
                                |    ON E.department_id = D.department_id
                                |WHERE E.employee_id <> D.employee_id
                                |ORDER BY  apellido, depart_id, depart_comp) AS table_1""")
    /**
      * Cargamos el resultado de la consulta en formato CSV en la carpeta del proyecto en HDFS para
      * su descarga y posterior envío.
      */

    val W_query6 = DF_query6.repartition(1).write.option("header","true").csv("/rrhh/peticiones_departamento_RRHH/empleado_y_compañeros.csv")

    /**
      * Séptima consulta: Se solicitan los datos de los empleados que entraron a trabajar después de Davies.
      * Cargamos la información necesaria a Spark.
      */

    val DF_query7 = spark.sql("""SELECT E.first_name,
                                |         E.Last_name,
                                |         E.hire_date
                                |FROM employees E
                                |JOIN employees DAVIES
                                |    ON davies.last_name = 'Davies'
                                |WHERE DAVIES.hire_date <= E.hire_date""")

    /**
      * Cargamos el resultado de la consulta en formato CSV en la carpeta del proyecto en HDFS para
      * su descarga y posterior envío.
      */
    val W_query7 = DF_query7.repartition(1).write.option("header","true").csv("/rrhh/peticiones_departamento_RRHH/empleados_despues_Davies.csv")

    /**
      * Octava consulta: Se solicitan los datos de los empleados que fueron contratados antes de sus managers.
      * Cargamos la información necesaria a Spark.
      */

    val DF_query8 = spark.sql("""SELECT E.last_name AS employee,
                                |         E.hire_date AS emp_hire_date,
                                |         M.last_name AS manager,
                                |         M.hire_date AS mgr_hire_date
                                |FROM employees E
                                |JOIN employees M
                                |    ON E.manager_id = M.employee_id
                                |WHERE E.hire_date < M.hire_date""")

    /**
      * Cargamos el resultado de la consulta en formato CSV en la carpeta del proyecto en HDFS para
      * su descarga y posterior envío.
      */

    val W_query8 = DF_query8.repartition(1).write.option("header","true").csv("/rrhh/peticiones_departamento_RRHH/empleados_antes_manager.csv")

    spark.stop()
  }
}
/* En siguientes versiones se implementará el código necesario para construir una aplicación Spark.*/
