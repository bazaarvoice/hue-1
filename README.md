![alt text](https://raw.githubusercontent.com/cloudera/hue/master/docs/images/hue_logo.png "Hue Logo")


Building locally
----------------
   * Follow the installation instructions [here](https://docs.gethue.com/administrator/installation/dependencies/#macos).
   
```
export LDFLAGS='-L/usr/local/opt/openssl/lib -L/usr/local/opt/gmp/lib'
xcode_sdk_path="$(xcrun --show-sdk-path -f)"
export CPPFLAGS='-I/usr/local/opt/openssl/include -I/usr/local/include -I'"$xcode_sdk_path"'/usr/include/sasl'
make apps SKIP_PYTHONDEV_CHECK=1
```

Helpful tips:
   *  `make clean apps` is your friend
   *  There may be an error w/ SSL certifcate handling in the build; if so, the 'apparent' problem may be the MacOS version. Successful builds have been based on MacOSX High Sierra and Catalina.


Query. Explore. Repeat.
-----------------------

Hue is an open source Analytic Workbench for browsing, querying and visualizing data with focus on SQL and Search: [gethue.com](http://gethue.com)

It features:

   * [Editors](https://gethue.com/new-sql-editor/) to query with SQL and submit jobs.
   * [Dashboards](https://gethue.com/improved-dashboard-layout/) to dynamically interact and visualize data.
   * [Scheduler](https://docs.gethue.com/user/scheduling/) of jobs and workflows.
   * [Browsers](https://docs.gethue.com/user/browsing/) for data and a Data Catalog.


![alt text](https://raw.githubusercontent.com/cloudera/hue/master/docs/images/sql-editor.png "Hue Editor")

![alt text](https://raw.githubusercontent.com/cloudera/hue/master/docs/images/dashboard.png "Hue Dashboard")


Who is using Hue
----------------
Thousands of companies and organizations use Hue to open-up and query their data in order to make smarter decisions. Just at Cloudera, Hue is heavily used by hundreds of customers executing millions of queries daily. Hue directly ships in Cloudera, Amazon, MapR, BigTop and is compatible with the other distributions.


Getting Started
---------------
Add the development packages, build and get the development server running:
```
git clone https://github.com/cloudera/hue.git
cd hue
make apps
build/env/bin/hue runserver
```
Now Hue should be running on [http://localhost:8000](http://localhost:8000) ! The configuration in development mode is ``desktop/conf/pseudo-distributed.ini``.

Read more in the [installation documentation](https://docs.gethue.com/administrator/installation/).


Docker
------
Start Hue in a single click with the [Docker Guide](https://github.com/cloudera/hue/tree/master/tools/docker) or the
[video blog post](http://gethue.com/getting-started-with-hue-in-2-minutes-with-docker/).


Community
-----------
   * User group: http://groups.google.com/a/cloudera.org/group/hue-user
   * Jira: https://issues.cloudera.org/browse/HUE
   * Reviews: https://review.cloudera.org/dashboard/?view=to-group&group=hue (repo 'hue-rw')


License
-----------
Apache License, Version 2.0
http://www.apache.org/licenses/LICENSE-2.0
