Knot mongo graph connector
==========================
![alt tag](http://spirtfire.com/res/img/knot-connector.png)
Real-time graph analysis on data in mongoDB

About
-----
This components is part of knot <https://github.com/homerquan/knot>: graph computing platform
It is inspired by mongo connector
<https://github.com/10gen-labs/mongo-connector>
and Write in nodejs. To sync data in real-time from collections in mongoDB into gremlin compatible graph DB (such as titan, neo4j). 

How to use
----------
1. change config (server/config/<env>.json) to specify which collections are vertieces or edges
2. run `node server/server.js`
3. DONE

Note: if it's first time run, it will replay all oplogs. Make sure all data are in oplogs, otherwise, a full restore may be needed. 

