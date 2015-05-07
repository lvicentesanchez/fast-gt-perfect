# fast-gt-perfect

### Pre-steps

First you need to start cassandra locally and create both the keyspace and the column families using `src/main/cql/01_key_space.cql` and `src/main/cql/02_column_family.cql`.

Then you need to create the folder `data` somewhere in your filesystem, i.e. your home folder. Inside the `data` you should create three folders named `revenue`, `trends` and `unique`.

Modify the config files `src/main/resources/revenue.conf`, `src/main/resources/trends.conf` and `src/main/resources/unique.conf` to replace `<path>` in the property `producer.folder` with the path to your data folder.

### Unique users

To run the unique users job, load sbt and execute the command:

`runMain com.mindcandy.data.jobs.unique.UniqueJobLauncher unique.conf`

In a different shell, copy the file `<repo>/fast-gt-perfect/data/unique/data.json` to the `unique` folder inside your `data` folder; Spark should detect the file and process it.

You can stop the Spark job by pressing the RETURN key at any moment.

### Revenue

To run the revenue job, load sbt and execute the command:

`runMain com.mindcandy.data.jobs.revenue.RevenueJobLauncher revenue.conf`

In a different shell, copy the file `<repo>/fast-gt-perfect/data/revenue/data.json` to the `revenue` folder inside your `data` folder; Spark should detect the file and process it.

You can stop the Spark job by pressing the RETURN key at any moment.

### Trends

To run the trending job, load sbt and execute the command:

`runMain com.mindcandy.data.jobs.trends.TrendsJobLauncher trends.conf`

In a different shell, copy the file `<repo>/fast-gt-perfect/data/trends/data.json` to the `trends` folder inside your `data` folder; Spark should detect the file and process it.

You can stop the Spark job by pressing the RETURN key at any moment.
