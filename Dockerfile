FROM public.ecr.aws/emr-serverless/spark/emr-6.15.0:latest

USER root

# 删除 emr 自带的jar
RUN rm -f /usr/lib/spark/jars/spark-redshift*

# MODIFICATIONS GO HERE
COPY assembly/target/datatunnel-3.4.0/*.jar /usr/lib/spark/jars/

#ADD https://repo1.maven.org/maven2/org/apache/kyuubi/kyuubi-extension-spark-3-4_2.12/1.8.0/kyuubi-extension-spark-3-4_2.12-1.9.0.jar /usr/lib/spark/jars/
COPY kyuubi-extension-spark-3-4_2.12-1.9.0.jar /usr/lib/spark/jars/

# EMRS will run the image as hadoop
USER hadoop:hadoop
