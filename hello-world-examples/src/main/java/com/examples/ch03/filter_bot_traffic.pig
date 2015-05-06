set mapred.cache.files 'blacklist.txt#blacklist';

set mapred.create.symlink 'yes';

register /Users/aahmed/SkyDrive/eclipse-projects/Hadoop/hello-world-examples/target/hello-world-examples-1.0-SNAPSHOT.jar;

all_weblogs = LOAD 'hdfs://master:9000/user/aahmed/apache_tsv.txt' AS (ip:chararray, timestamp:long, page:chararray, http_status:int, payload_size:int, useragent:chararray);

nobots_weblogs = FILTER all_weblogs BY NOT com.examples.ch03.IsUseragentBot(useragent);

STORE nobots_weblogs INTO 'hdfs://master:9000/user/aahmed/nobots_weblogs';

//pig -f /Users/aahmed/SkyDrive/eclipse-projects/Hadoop/hello-world-examples/src/main/java/com/examples/ch03/filter_bot_traffic.pig