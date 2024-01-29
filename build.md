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

### 构建AWS EMR Serverless镜像(AMD64)
```
docker logout public.ecr.aws
aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin 480976988805.dkr.ecr.us-east-1.amazonaws.com

docker buildx build --platform linux/amd64 -t emr6.15-serverless-spark .
docker tag emr6.15-serverless-spark:latest 480976988805.dkr.ecr.us-east-1.amazonaws.com/emr6.15-serverless-spark:latest
docker push 480976988805.dkr.ecr.us-east-1.amazonaws.com/emr6.15-serverless-spark:latest
```