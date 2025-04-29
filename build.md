## Build

#### 独立集成spark 打包
```
-- antlr4 版本要与spark 中版本一致
mvn clean spotless:apply package -DlibScope=provided -Dmaven.test.skip=true -Phadoop3
mvn clean spotless:apply package -DlibScope=provided -Dmaven.test.skip=true -Pcdh6
```

### 构建AWS EMR Serverless镜像(AMD64) (美东)
```
docker logout public.ecr.aws
aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin 654654620044.dkr.ecr.us-east-1.amazonaws.com
docker buildx build -f Dockerfile-AWS --platform linux/amd64 -t cyberdata/emr-spark .
docker tag cyberdata/emr-spark:latest 654654620044.dkr.ecr.us-east-1.amazonaws.com/cyberdata/emr-spark:latest
docker push 654654620044.dkr.ecr.us-east-1.amazonaws.com/cyberdata/emr-spark:latest
```

ruixin image (新加坡)
```
docker logout public.ecr.aws
aws ecr get-login-password --region ap-southeast-1 --profile ruixin_southeast | docker login --username AWS --password-stdin 753463419839.dkr.ecr.ap-southeast-1.amazonaws.com

docker buildx build -f Dockerfile-AWS --platform linux/amd64 -t cyberdata .
docker tag cyberdata:latest 753463419839.dkr.ecr.ap-southeast-1.amazonaws.com/cyberdata:latest
docker push 753463419839.dkr.ecr.ap-southeast-1.amazonaws.com/cyberdata:latest
```

ruixin image (美东)
```
docker logout public.ecr.aws
aws ecr get-login-password --region us-east-1 --profile ruixin_us_east | docker login --username AWS --password-stdin 257394478466.dkr.ecr.us-east-1.amazonaws.com/cyberdata

docker buildx build -f Dockerfile-AWS --platform linux/amd64 -t cyberdata .
docker tag cyberdata:latest 257394478466.dkr.ecr.us-east-1.amazonaws.com/cyberdata:latest
docker push 257394478466.dkr.ecr.us-east-1.amazonaws.com/cyberdata:latest

```

redtiger image (美东)
```
# 需要权限: ecr:InitiateLayerUpload 
docker logout public.ecr.aws
aws ecr get-login-password --region us-east-1 --profile redtiger | docker login --username AWS --password-stdin 537124946315.dkr.ecr.us-east-1.amazonaws.com

docker buildx build -f Dockerfile-AWS --platform linux/amd64 -t data-warehouse/cyberdata .
docker tag data-warehouse/cyberdata:latest 537124946315.dkr.ecr.us-east-1.amazonaws.com/data-warehouse/cyberdata:latest
docker push 537124946315.dkr.ecr.us-east-1.amazonaws.com/data-warehouse/cyberdata:latest
```

cyberdata image
```
docker login --username admin --password-stdin 172.88.0.30:5220
docker buildx build -f Dockerfile-Apache --platform linux/amd64 -t cyberdata-spark:3.5 .
docker tag cyberdata-spark:3.5 172.88.0.30:5220/deps/cyberdata-spark:3.5
docker push 172.88.0.30:5220/deps/cyberdata-spark:3.5
```

### EMR Serverless ECR 仓库镜像需要添加权限

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "ECRRepositoryPolicy-EMRServerless",
      "Effect": "Allow",
      "Principal": {
        "Service": "emr-serverless.amazonaws.com",
        "AWS": "提交用户ARN"
      },
      "Action": [
        "ecr:BatchCheckLayerAvailability",
        "ecr:BatchGetImage",
        "ecr:CompleteLayerUpload",
        "ecr:DescribeImages",
        "ecr:DescribeRepositories",
        "ecr:GetDownloadUrlForLayer",
        "ecr:InitiateLayerUpload",
        "ecr:PutImage",
        "ecr:UploadLayerPart"
      ]
    }
  ]
}
```