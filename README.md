# hogwild-spark

## Abstract

The goal of this project is to design, implement and experiment a synchronous version in Spark of a distributed stochastic gradient descent (SGD) used in Support Vector Machines (SVMs) by comparing it with previous synchronous and asynchronous implementations in Python. 

The main reference for this project is the [Hogwild! paper](https://people.eecs.berkeley.edu/~brecht/papers/hogwildTR.pdf). The Hogwild! paper is an important paper in the Machine Learning and Parallel Computing community that shows that SGD can be implemented without any locking when the associated optimization problem is sparse. [hogwild-python](https://github.com/liabifano/hogwild-python) is a synchronous and asynchronous implementation in Python of the Hogwild! algorithm by EPFL students. This project is part of the [*CS-449 Systems for Data Science*](http://edu.epfl.ch/coursebook/en/systems-for-data-science-CS-449) course taught at EPFL in the Spring semester of 2019.
