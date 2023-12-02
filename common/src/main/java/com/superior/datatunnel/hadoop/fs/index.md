<!---
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License. See accompanying LICENSE file.
-->

Hadoop ftp/ftps/sftp filesystem
======================

Backport, rewrite and improvements of the current ftp filesystem and of hadoop sftp filesystem from Hadoop 2.8

New features:
-----
* Support for HTTP/SOCKS proxies
* Support for passive FTP
* Support for explicit FTPS
* Support of connection pooling - new connection is not created for every single command but reused from the pool.
For huge number of files it shows order of magnitude performance improvement over not pooled connections.
* Caching of directory trees. For ftp you always need to list whole directory whenever you ask information about particular file.
Again for huge number of files it shows order of magnitude performance improvement over not cached connections.
* Support of keep alive (NOOP) messages to avoid connection drops
* Support for Unix style or regexp wildcard glob - useful for listing a particular files across whole directory tree
* Support for reestablishing broken ftp data transfers - can happen surprisingly often
* Support for sftp private keys (including pass phrase)
* Support for keeping passwords, private keys and pass phrase in the jceks key stores

Schemas and theirs implementing classes
-------
FTP:  org.apache.hadoop.fs.ftpextended.ftp.FTPFileSystem
FTPS: org.apache.hadoop.fs.ftpextended.ftp.FTPFileSystem
SFTP: org.apache.hadoop.fs.ftpextended.sftp.SFTPFileSystem

Sample usage
-----
```
hadoop distcp
            -Dfs.<schema>.password.<hostname>.<user>=<passwd>
            -Dfs.<schema>.impl=<FQN of implementing class>
            <schema>://<user>@<hostname>/<src_file> <dst_file>

hdfs dfs
    -Dfs.<schema>.impl=<FQN of implementing class> -ls  <schema>://<user>@<hostname>/
```



Properties
-----
* fs.\<schema\>.connection.max - max number of parallel connections stored in connection pool
    * values: int
    * default: 5
* fs.\<schema\>.host - server host name
* fs.\<schema\>.host.port - server port
    * values: int
    * default: FTP 21, SFTP 22
* fs.\<schema\>.user.\<hostname\> - user name used connecting to \<hostname\>
    * values: string
    * default: anonymous
* fs.\<schema\>.password.\<hostname\>.\<user\> - password used when connecting as \<user\> to \<hostname\>
    * values: string
    * default: anonymous@domain.com
* hadoop.security.credential.provider.path - location of jceks keystore, the same as using -provider parameter for hadoop commands
    * values: string
* fs.\<schema\>.key.file.\<hostname\>.\<user\> - URI of private key used to conect as \<user\> to \<hostname\>
    * values: URI string
* fs.\<schema\>.key.passphrase.\<hostname\>.\<user\> - Pass phrase used to decrypt private key
    * values: string
* fs.\<schema\>.proxy.type - type of proxy
    * values: enum - NONE/HTTP/SOCKS4/SOCKS5
    * default: NONE
* fs.\<schema\>.proxy.host - proxy host name
* fs.\<schema\>.proxy.port - proxy port
    * values: int
    * default: HTTP 8080, SOCKS 1080
* fs.\<schema\>.proxy.user - proxy user name
* fs.\<schema\>.proxy.password - proxy user password
* fs.\<schema\>.glob.type - how to process search queries (wildcard processing)
    * values: enum - UNIX/REGEXP
    * default: UNIX
* fs.\<schema\>.cache.\<hostname\> - cache accessed files and directories for particular hostname, can significantly improves performance for big file trees
    * values: boolean
    * default: false
* fs.\<schema\>.use.keepalive - prevent connection cancelation by sending NOOP commands
    * values: boolean
    * default: false
* fs.\<schema\>.use.keepalive.period - period for sending NOOP in minutes
    * values: int
    * default: 1

Supported proxies
-----------------
FTP: HTTP
FTPS: NONE
SFTP: HTTP, SOCKS4, SOCKS5

Notes on password/keys management
=================================
Passwords and private keys with/without pass phrase can be supplied both on command line and in jceks key store or combination of them.
####File location
JCEKS files and private ley files must be accessible on all nodes which uses this file system.
Recommended location is therefore HDFS
####Priority rules
command line parameter values has precedence over key store values
password has priority over key file
####JCEKS aliases
password: \<hostname\>_\<user\>_password
private key: \<hostname\>_\<user\>_key
key passphrase: \<hostname\>_\<user\>_key_passphrase
####Adding password or pass phrase to the key store
By hadoop credentials utility
hadoop credential create \<hostname\>_\<user\>_password -provider jceks://<keystore_location\>/\<keystore\>.jceks
hadoop credential create \<hostname\>_\<user\>_key_passphrase -provider jceks://<keystore_location\>/\<keystore\>.jceks

By keytool utility (requires not default password - see [Keystore access password](#keystore-access-password)
keytool -importpassword  -alias \<hostname\>_\<user\>_password -keystore \<keystore\>.jceks -storetype JCEKS
keytool -importpassword  -alias \<hostname\>_\<user\>_key_passphrase -keystore \<keystore\>.jceks -storetype JCEKS

####Adding private key to the keystore
Hadoop credentials utility can work only with the SecretKeyEntry not with the PrivateKeyEntry. To add the private key entry to the keystore you can use following series of commands:
openssl req -new -x509 -key \<private key file\> -out \<key certificate\>
openssl pkcs12 -export -out \<keystore\>.pk12 -inkey \<private key file\> -in \<key certificate\>
keytool -importkeystore  -alias 1 -destalias \<hostname\>_\<user\>_key -v -srckeystore \<keystore\>.pk12 -srcstoretype PKCS12 -destkeystore \<keystore\>.jceks -deststoretype JCEKS
####Keystore access password
Other limitation of hadoop credentials implementation is that it is using a default keystore password which is too short and not accepted by the keytool utility. Therefore when creating a key store containing the private key, you must specify your own password and than pass it to the all nodes using the keys store.
That can be done either by specifying an environment variable: HADOOP_CREDSTORE_PASSWORD
Either by defining property *hadoop.security.credstore.java-keystore-provider.password-file* which contains location of a file containing the password. See more in https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-common/CredentialProviderAPI.html

Contract integration test
-------------------------
Contract integration tests will by default use built in ftp/sftp test server. If you would like to use different servers please modify src/test/resources/contract/params.xml file so properties fs.contract.use.internal.ftpserver resp. fs.contract.use.internal.sftpserver are set to false and specify the proper endpoints in src/test/resources/contract-test-options.xml


TODO
----
SOCKS support for ftp

add support for appendFile

add support for CWD

