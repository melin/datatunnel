FROM public.ecr.aws/w6m0k7l2/spark:spark-3.4.2

COPY assembly/target/datatunnel-3.4.0/*.jar /opt/spark/jars/

#ADD https://repo1.maven.org/maven2/org/apache/hudi/hudi-spark3.4-bundle_2.12/0.14.1/hudi-spark3.4-bundle_2.12-0.14.1.jar /opt/spark/jars
#ADD https://repo1.maven.org/maven2/org/apache/kyuubi/kyuubi-extension-spark-3-4_2.12/1.8.0/kyuubi-extension-spark-3-4_2.12-1.8.0.jar /usr/lib/spark/jars/
COPY *.jar /opt/spark/jars/
