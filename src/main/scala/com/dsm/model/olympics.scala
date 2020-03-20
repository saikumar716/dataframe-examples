package com.dsm.model

import org.apache.spark.sql.types.DateType

case class olympics(Athlete: String,
                    Age: Int,
                    Country: String,
                    Year: Double,
                    Closing_Date: String,
                    Sport: String,
                    Gold_Medals: Double,
                    Silver_Medals: Double,
                    Bronze_Medals: Double,
                    Total_Medals: Double)
