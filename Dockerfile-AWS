FROM public.ecr.aws/emr-serverless/spark/emr-7.1.0:latest

USER root

# 删除 emr 自带的jar
RUN rm -f /usr/lib/spark/jars/spark-redshift*

# MODIFICATIONS GO HERE
COPY assembly/target/datatunnel-3.5.0/*.jar /usr/lib/spark/jars/

COPY kyuubi-extension-spark-3-5_2.12-1.9.1.jar /usr/lib/spark/jars/

# EMRS will run the image as hadoop
USER hadoop:hadoop
