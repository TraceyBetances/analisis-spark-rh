package peticiones

import org.apache.spark.sql.SparkSession

object master_querys {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    val crtl_querys = new RRHH_requests(args(0),n_q, spark)

    val flagControl_DF_query1 = true
    val flagControl_DF_query2 = true
    val flagControl_DF_query3 = false
    val flagControl_DF_query4 = false
    val flagControl_DF_query5 = false
    val flagControl_DF_query6 = false
    val flagControl_DF_query7 = false
    val flagControl_DF_query8 = false


    if (flagControl_DF_query1) {
      //Control sae_dwc
      crtl_querys.DF_query1()
    }

    if (flagControl_DF_query2) {
      //Control sae_dwc
      crtl_querys.DF_query2()
    }

    if (flagControl_DF_query3) {
      //Control sae_dwc
      crtl_querys.DF_query3()
    }

    if (flagControl_DF_query4) {
      //Control sae_dwc
      crtl_querys.DF_query4()
    }

    if (flagControl_DF_query5) {
      //Control sae_dwc
      crtl_querys.DF_query5()
    }

    if (flagControl_DF_query6) {
      //Control sae_dwc
      crtl_querys.DF_query6()
    }

    if (flagControl_DF_query7) {
      //Control sae_dwc
      crtl_querys.DF_query2()
    }

    if (flagControl_DF_query7) {
      //Control sae_dwc
      crtl_querys.DF_query2()
    }

    if (flagControl_DF_query8) {
      //Control sae_dwc
      crtl_querys.DF_query8()
    }
  }
}

