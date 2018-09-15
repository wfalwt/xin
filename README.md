# xin
Xin is a framework example to import data from RDBMS to HBase, currently MySQL is the only option.

## usage arguments

> -t/--type    data scope,need program extends scope
> -u/--user    MySQL username to connect
> -e/--pwd     MySQL password to connect
> -d/--db      MySQL database to use
> -h/--host    MySQL host address to connect
> -p/--port    MySQL port,default 3306
> -q/--quorum  HBase zookeeper quorum address

## usage in cli
```sh
host=10.0.0.4
user=someuser
pwd=pwdtouser
db=somedb
quorum=zk-cluster:2181
java -jar xin-1.0.jar -t users -d $db -h $host -u $user -e $pwd -q $quorum
```
