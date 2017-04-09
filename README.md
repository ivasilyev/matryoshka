# matryoshka
A wrapper that wraps the wrappers.

**[YO DAWG](http://knowyourmeme.com/memes/xzibit-yo-dawg) I HEARD THAT YOU LIKE WRAPPERS, SO I PUT A WRAPPER INTO A WRAPPER, SO YOU CAN PARALLELIZE WHILE YOU PARALLELIZE!**

```
python3 matryoshka.py  -i/--input <file> -o/--output <dir> -w/--wait <bool> -t/--threads <int> -n/--nodes <str> -s/--string <str>
```
Available processing  modes:
- Remote multi-node multi-thread "burn-your-mainframe" mode;
- Remote multi-node single-thread mode;
- Remote single-node multi-thread mode;
- Remote single-node single-thread mode;
- Local multi-thread mode;
- Local single-thread "don't-like-bash" mode

Requires installed *python 3.3+* and [*Paramiko*](http://www.paramiko.org/) module.
The usage example:
```
matryoshka.py -i var_table.txt -o result_dir -t 8 -n "node9,node7,node5" -s "myprogram \$0 constant1 \$1 constant2"
```
Suppose the *var_table.txt* file contains no header, 2 tab-delimited columns and 30 tab-delimited rows of different parameters:
```
param101	param201
param102	param202
...
param130	param230
```
In this case, 3 wrappers loaded with all required data will be replicated into result_dir directory and simultaneously launched on node9, node7 and node5 nodes one per node via SSH. Each will be optimized to work at 8 threads.
There is also support for per-node SSH authentication: 
```
... -n /secret/ssh_hosts.txt ...
```
The text file must contain all required information: node name, user name (default is empty), password (default is empty), port (default is 22) divided by colon:
```
open_nodeA:::
open_nodeB:::12000
...
secure_node:username:password:22
```
When the preassigned authentication parameters fail, the program will attempt an to log in anonymously, and automatically filter it after the failure. It will shut down if there would not be available nodes in input.
Do not forget to get rid of master logs containing important information like SSH keys. 
Without -n parameter it will run on local machine. It is equal to doing single-type operations 30 times:
```
myprogram param101 constant1 param201 constant2
myprogram param102 constant1 param202 constant2
...
myprogram param130 constant1 param230 constant2
```
With *-w* parameter the tool also may be implemented as a basis of fast pipeline even with bash:
```
matryoshka.py -i table1.txt -o dir1 -w -t 8 -n "node1,node2,node3" -s "tool1 \$0 param11 \$1 param12"
matryoshka.py -i table2.txt -o dir2 -w -n "node1,node2,node3" -s "tool2 \$0 constant21 \$1 constant2 \$22"
matryoshka.py -i table3.txt -o dir0 -w 8 -n "node1,node2,node3 -s "tool3 \$0 constant31 constant32"
```
The program requires disk space shared between all using nodes.
