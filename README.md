# The Byzantine General’s problem with Consensus
Distributed Systems Mini-Project 2

---

## Requirements
* Python==3.8
* rpyc==5.0.1
  * ```pip install rpyc==5.0.1```

---

## Instructions

To start the program clone this repository and run the program with (inside the directory of the repository):

```
python main.py N
```

where ```N``` is the number of processes to be created.

By default the RPC servers of the processes start from ```starting_port=18812```.
For example, processes are assigned ports ```starting_port```, ```starting_port + 1```,```...``` , ```starting_port + N - 1```,
where ```N``` is the number of processes created.

The starting port can changed, for example:
```
python main.py 3 --starting-port 10010
```

When started, the program asks for user input. 
Valid user inputs are as in the instructions (see *Mini-project2-DS2022.pdf*).





