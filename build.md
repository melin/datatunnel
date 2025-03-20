## Build

#### 独立集成spark 打包
```
-- antlr4 版本要与spark 中版本一致
mvn clean spotless:apply package -DlibScope=provided -Dmaven.test.skip=true -Pcdh6
mvn clean spotless:apply package -DlibScope=provided -Dmaven.test.skip=true -Phadoop3
```

### 构建AWS EMR Serverless镜像(AMD64)
```
docker logout public.ecr.aws
aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin 480976988805.dkr.ecr.us-east-1.amazonaws.com

docker buildx build -f Dockerfile-AWS --platform linux/amd64 -t emr6.15-serverless-spark .
docker tag emr6.15-serverless-spark:latest 480976988805.dkr.ecr.us-east-1.amazonaws.com/emr6.15-serverless-spark:latest
docker push 480976988805.dkr.ecr.us-east-1.amazonaws.com/emr6.15-serverless-spark:latest
```

ruixin image
```
docker logout public.ecr.aws
aws ecr get-login-password --region ap-southeast-1 | docker login --username AWS --password-stdin 753463419839.dkr.ecr.ap-southeast-1.amazonaws.com

docker buildx build -f Dockerfile-AWS --platform linux/amd64 -t cyberdata .
docker tag cyberdata:latest 753463419839.dkr.ecr.ap-southeast-1.amazonaws.com/cyberdata:latest
docker push 753463419839.dkr.ecr.ap-southeast-1.amazonaws.com/cyberdata:latest
```

cyberdata image
```
docker login --username admin --password-stdin 172.88.0.30:5220
docker buildx build -f Dockerfile-Apache --platform linux/amd64 -t cyberdata-spark:3.5 .
docker tag cyberdata-spark:3.5 172.88.0.30:5220/deps/cyberdata-spark:3.5
docker push 172.88.0.30:5220/deps/cyberdata-spark:3.5
```