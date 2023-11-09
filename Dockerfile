FROM public.ecr.aws/emr-serverless/spark/emr-6.14.0:latest

USER root
# MODIFICATIONS GO HERE
COPY assembly/target/datatunnel-3.3.0-SNAPSHOT/*.jar /usr/lib/spark/jars/
# EMRS will run the image as hadoop
USER hadoop:hadoop