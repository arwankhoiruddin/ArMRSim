# MapReduce Simulator

This is a MapReduce simulator used in Arwan Ahmad Khoiruddin's PhD at Universiti Technology PETRONAS, Malaysia. 

The goals of the simulator is to simulate low-level work of MapReduce especially the scheduling part. Thus, we can easily design and prerun our scheduling algorithm in this platform before implementing it in the real MapReduce system.

## This simulator DO the followings

* Simulate homogeneous and heterogeneous MapReduce cluster
* Simulate MapReduce operation. The tasks are generated randomly. We want to see MapReduce in more general manner, not on running certain kind of task

## This simulator DON'T do the followings

* Run specific MapReduce tasks (e.g. WordCount)

## How randomness is used in this simulator?

* Zipf distribution is used to generate  the length of the Map and Reduce tasks
* Normal random distribution is used to generate other things that needs to be random. We use `java.util.Random` for this need 

If you are interested in this project, feel free to join