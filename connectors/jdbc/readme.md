https://dzlab.github.io/spark/2022/02/10/spark-jdbc-partitioning/

def dataFrameToDDL(dataFrame: DataFrame, tableName: String): String = {
val columns = dataFrame.schema.map { field =>
"  " + field.name + " " + field.dataType.simpleString.toUpperCase
}
s"CREATE TABLE $tableName (\n${columns.mkString(",\n")}\n)"
}