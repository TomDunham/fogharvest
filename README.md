FogHarvest submits time logged against Fogbugz tickets to Harvest timesheets.

Operation is rudimentary: all time logged to tickets is submitted to a
task called "Development". The notes for the time entry contain the
case number and its title.

* No attempt is made to limit API requests.
* When in debug mode willfully prints your password all over the
  screen and logfile in plain text.
* Makes no attempt to detect if time has already been posted (expect
  duplicates if you run it more than once a day).


Installation
------------

Clone the repository into a directory on a server that can reach your
Fogbugz and Harvest servers.


Usage
-----

*Requires* Python 2.7

`python fogharvest.py`

This will process yesterday's timesheets. It is possible to control
what time period is processed, and for which users; run `fogharvest.py
-h` for more details.


Config
------

You'll need a `fogharvest.cfg` in the directory from which you launch
fogharvest. It contains the urls of your Fogbugz and Harvest
instances, usernames and passwords. There is an example
(`fogharvest.cfg.example`) in the checkout.


Debugging
---------

Run `fogharvest.py --debug` to dump masses of info to the terminal.
Add the `--dry-run` switch if you want to see what will be posted
without posting it.
