scp assembly/target/datatunnel-3.4.0.tar.gz root@172.18.5.46:/root
superior2023

kinit -kt /root/superior.keytab superior/admin@DATACYBER.COM
rm -rf datatunnel-3.4.0
tar -zxf datatunnel-3.4.0.tar.gz
cd datatunnel-3.4.0
hdfs dfs -rm /user/superior/spark-jobserver/datatunnel-3.4.0/*
hdfs dfs -put *.jar /user/superior/spark-jobserver/datatunnel-3.4.0/
cd /root

