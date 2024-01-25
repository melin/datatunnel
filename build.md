## Build

#### 独立集成spark 打包
```
-- antlr4 版本要与spark 中版本一致
mvn clean package -DlibScope=provided -Dmaven.test.skip=true -Pcdh6
mvn clean package -DlibScope=provided -Dmaven.test.skip=true -Phadoop3
mvn clean package -DlibScope=provided -Dmaven.test.skip=true -Phadoop2
```

#### superior 平台打包，排除一些平台已经有的jar
```
mvn clean package -DlibScope=provided -Dsuperior.libScope=provided -Dmaven.test.skip=true -Pcdh6
mvn clean package -DlibScope=provided -Dsuperior.libScope=provided -Dmaven.test.skip=true -Phadoop3
mvn clean package -DlibScope=provided -Dsuperior.libScope=provided -Dmaven.test.skip=true -Phadoop2
```

### 构建Docker镜像(ARM)
```
docker logout public.ecr.aws
aws ecr-public get-login-password --region us-east-1 | docker login --username AWS --password-stdin public.ecr.aws/w6m0k7l2

docker build -t spark-datatunnel:3.4.2 .
docker tag spark-datatunnel:3.4.2 public.ecr.aws/w6m0k7l2/spark-datatunnel:3.4.2
docker push public.ecr.aws/w6m0k7l2/spark-datatunnel:3.4.2
```

### 构建Spark 镜像
```
aws ecr-public get-login-password --region us-east-1 | docker login --username AWS --password-stdin public.ecr.aws/w6m0k7l2

./bin/docker-image-tool.sh -r public.ecr.aws/w6m0k7l2 -t spark-3.4.2 build
./bin/docker-image-tool.sh -r public.ecr.aws/w6m0k7l2 -t spark-3.4.2 push

-- 提交 examples
./bin/spark-submit \
    --master k8s://https://xxxx.yl4.us-east-1.eks.amazonaws.com \
    --deploy-mode cluster \
    --name spark-pi \
    --class org.apache.spark.examples.SparkPi \
    --conf spark.executor.instances=1 \
    --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark \
    --conf spark.kubernetes.container.image=public.ecr.aws/w6m0k7l2/spark:spark-3.4.2 \
    --conf spark.kubernetes.file.upload.path=s3a://superior2023/kubenetes \
	--conf spark.hadoop.fs.s3a.access.key=xxx \
	--conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem  \
	--conf spark.hadoop.fs.s3a.fast.upload=true  \
	--conf spark.hadoop.fs.s3a.secret.key=xxx  \
    s3a://superior2023/spark/spark-examples_2.12-3.4.2.jar 5
```

### 构建AWS EMR Serverless镜像(AMD64)
```
docker logout public.ecr.aws
aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin 480976988805.dkr.ecr.us-east-1.amazonaws.com

docker buildx build --platform linux/amd64 -f Dockerfile-EMR -t emr6.15-serverless-spark .
docker tag emr6.15-serverless-spark:latest 480976988805.dkr.ecr.us-east-1.amazonaws.com/emr6.15-serverless-spark:latest
docker push 480976988805.dkr.ecr.us-east-1.amazonaws.com/emr6.15-serverless-spark:latest
```