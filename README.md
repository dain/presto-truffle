presto-truffle
==============

This is a test of the performance of Truffle compared to pure Java code.

The test generates TPC-H data in memory and executes the TPC-H query 6:
```
select sum(extendedprice * discount) as revenue      
from lineitem                                        
where shipdate >= '1994-01-01'                       
   and shipdate < '1995-01-01'                       
   and discount >= 0.05                              
   and discount <= 0.07                              
   and quantity < 24;                                
```

This test is excuted using pure Java code and with a hand-crafted Truffle AST modelling this code.

Setup
=====

After building a Graal VM run the following commands in the Graal directory 
to publish the Truffle artifacts to the local maven repository
```
mvn install:install-file -Dfile=./graal/com.oracle.truffle.api/com.oracle.truffle.api.jar -DgroupId=com.oracle.truffle -DartifactId=truffle-api -Dversion=1.0-SNAPSHOT -Dpackaging=jar
mvn install:install-file -Dfile=./graal/com.oracle.truffle.api.dsl/com.oracle.truffle.api.dsl.jar -DgroupId=com.oracle.truffle -DartifactId=truffle-api-dsl -Dversion=1.0-SNAPSHOT -Dpackaging=jar
mvn install:install-file -Dfile=./graal/com.oracle.truffle.dsl.processor/com.oracle.truffle.dsl.processor.jar -DgroupId=com.oracle.truffle -DartifactId=truffle-dsl-processor -Dversion=1.0-SNAPSHOT -Dpackaging=jar
```

Execution
=========

Set the `GRAAL_HOME` environment variable to point to your Graal VM.  For, example:

```
export GRAAL_HOME=~/work/graal/jdk1.7.0_45/product/
```

After performing a `maven install` build, run the following from the presto-truffle directory to run the tests:

```
$GRAAL_HOME/bin/java -server -G:TruffleCompilationThreshold=10 -Xmx4g -cp target/presto-truffle-1.0-SNAPSHOT.jar:$HOME/.m2/repository/io/airlift/slice/0.2/slice-0.2.jar:$HOME/.m2/repository/com/google/guava/guava/14.0.1/guava-14.0.1.jar:$HOME/.m2/repository/com/oracle/truffle/truffle-api/1.0-SNAPSHOT/truffle-api-1.0-SNAPSHOT.jar com.facebook.presto.truffle.PureJavaTest
```

```
$GRAAL_HOME/bin/java -server -G:TruffleCompilationThreshold=10 -Xmx4g -cp target/presto-truffle-1.0-SNAPSHOT.jar:$HOME/.m2/repository/io/airlift/slice/0.2/slice-0.2.jar:$HOME/.m2/repository/com/google/guava/guava/14.0.1/guava-14.0.1.jar:$HOME/.m2/repository/com/oracle/truffle/truffle-api/1.0-SNAPSHOT/truffle-api-1.0-SNAPSHOT.jar com.facebook.presto.truffle.TruffleTest
```

After about 4 iterations, Truffle will switch from the interpreter to compiled code and performance will improve significantly.


Evaluation
==========
PureJava (-original):    177.85ms
PureJava (-server):      178.26ms
PureJava (-graal):       126.15ms
TruffleTest (-original): 378.23ms
TruffleTest (-server):    88.22ms
TruffleTest (-graal):     87.85ms

