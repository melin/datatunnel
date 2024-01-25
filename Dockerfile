FROM public.ecr.aws/w6m0k7l2/spark:spark-3.4.2

COPY assembly/target/datatunnel-3.4.0/*.jar /usr/lib/spark/jars/
