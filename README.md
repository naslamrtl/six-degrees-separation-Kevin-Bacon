# six-degrees-separation-Kevin-Bacon

The goal of this programe is to perform specific data analytics on Kevin Bacon to identify the actors
(both male and female) linked to him by the six degrees of separation (more info can be found on
the Wikipedia page “Six Degrees of Kevin Bacon”).
First you need to download the files actors.list.gz and actresses.list.gz from the following link
ftp://ftp.fu-berlin.de/pub/msc/movies/database/ 

The algorithm that I followed (for rdd manipulation )is as below: 

Create an RDD of type <actor, List<movies>> by using the newAPIHadoopFile function.

From that RDD, generate an RDD with actors and the movies they acted in (<actor, movie>).

Next create an RDD for actors and their collaborating actors (<actor, actor>).

Reduce that RDD into an RDD of type <actor, list<actor>>.

Create a new RDD describing actors and their distances to Kevin Bacon (<actor, distance>),
where distance=infinity, except for Kevin Bacon for whom distance is 0.

Join the previous RDDs into a new one showing the collaborating actors with a given actor at
a specific distance (<actor, <distance, list<actor>>>).

Use this list to generate an RDD of type <actor, distance+1>.

Reduce this list by taking the minimum function of the distance (<actor, minDistance>).

Iterate 6 times and identify the list of actors (both male and female) at a distance of 6 from
Kevin Bacon. For example, after the first iteration, Bacon will have a distance of 0 and every
actor who worked with him would be at distance 1, while the rest of the actors would be at
distance of infinite. This connectivity would spread in the next iterations.
