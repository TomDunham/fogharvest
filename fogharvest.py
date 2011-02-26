#!/usr/bin/env python
"""
Forward time logged against tickets in FogBugz to Harvest.
"""
import argparse
import sys
import urllib, urlparse, urllib2
import datetime
import xml.etree.ElementTree as ET
import logging
from ConfigParser import RawConfigParser
from base64 import b64encode
from collections import namedtuple
from StringIO import StringIO
from itertools import groupby

logger = logging.getLogger("main")
logger.addHandler(logging.NullHandler())

def pgroup(itr, key, skey=None):
    "Print itr sorted by skey grouped by key. If skey not set, group & sort by key"
    for k, grp in sgroup(itr, key, skey):
        print k, "=" * 60
        print "\n".join("%r" % i for i in grp)

def sgroup(itr, key, skey=None):
    "sort itr by skey and group by key. If skey not set, group & sort by key"
    if not skey:
        skey = key
    return groupby(sorted(itr, key=skey), key=key)

def timestamp(ts):
    return datetime.datetime.strptime(ts, "%Y-%m-%dT%H:%M:%SZ")

def datestamp(dt):
    return datetime.datetime.strptime(dt, "%Y-%m-%d")

def boolstr(bs):
    return bs.lower == u"true"

class Storage(object):
    """
    Mutable bag of values, can access by key or attrib.

    >>> s = Storage({"a": 1})
    >>> s.a
    1
    >>> s["a"]
    1
    """
    def __init__(self, mapping):
        self.__dict__.update(mapping)

    def __repr__(self):
        return "Storage(%r)" % self.__dict__

    def get(self, k, default=None):
        try:
            return self[k]
        except KeyError:
            return default

    def __getitem__(self, k):
        return self.__dict__[k]

    def __setitem__(self, k, v):
        self.__dict__[k] = v


## Handle xml from APIs in a <tag>value</tag> form.
# Eg:
# <intervals><ixInterval>2</ixInterval><sTitle>Fix Bug</sTitle></intervals>

class APINode(object):
    @classmethod
    def parse(cls, el):
        vals = { attr : conv(el.find(tag).text.strip()) for tag, (attr, conv) in cls.emap.items() }
        return cls(**vals)

def api_node(name, emap):
    return type(name, (APINode, namedtuple(name, " ".join(k[0] for k in emap.values()))), { "emap" : emap })

class API(object):
    def parse_resp(self, resp, node_tag, node_cls):
        "Parse an xml response into a node class."
        return list(node_cls.parse(e) for e in ET.parse(resp).findall(".//" + node_tag))

## Node (record) classes for Fogbugz

class FB(API):
    "The Fogbugz API"

    Interval = api_node("Interval", {
        u"ixInterval" : ("id", int),
        u"ixPerson" : ("person_id", int),
        u"ixBug" : ("bug_id", int),
        u"dtStart" : ("start", timestamp),
        u"dtEnd" : ("end", timestamp),
        u"sTitle" : ("title", unicode),
        u"fDeleted" : ("deleted", boolstr)
        })

    Person = api_node("Person", {
        u"ixPerson" : ("id", int),
        u"sFullName" : ("full_name", unicode),
        u"sEmail" : ("email", unicode),
        u"dtLastActivity" : ("last_activity", timestamp)
        })

    Case = api_node("Case", {
        u"ixBug" : ("id", int),
        u"ixProject" : ("project_id", int),
        u"sProject" : ("project_name", unicode),
    })

    def __init__(self, url, email, password):
        super(FB, self).__init__()
        self.url = url
        self.email = email
        self.password = password
        self.api_url = None
        self.token = None

    def logon(self):
        api_xml = self.open(urlparse.urljoin(self.url, "api.xml"))
        api_ep = ET.parse(api_xml).find("url").text
        self.api_url = urlparse.urljoin(self.url, api_ep)
        token_resp = self.call("logon", email=self.email, password=self.password)
        self.token = ET.parse(token_resp).find("token").text

    def logon_if_required(self):
        if not self.token:
            self.logon()

    def cmd_url(self, cmd, **args):
        if cmd != "logon":
            if not self.token:
                raise ValueError("token not set - must login")
            args["token"] = self.token
        args["cmd"] = cmd
        return urlparse.urljoin(
            self.api_url, "?" + urllib.urlencode(args))

    def open(self, url):
        logger.debug("open url=%r", url)
        resp = urllib2.urlopen(url)
        resp_str = resp.read()
        logger.debug("response (len %r)", len(resp_str))
        return StringIO(resp_str)

    def call(self, cmd, **args):
        return self.open(self.cmd_url(cmd, **args))

    def intervals(self, start=None):
        self.logon_if_required()
        args = {}
        if start:
            args = { "dtStart" : start.strftime("%Y-%m-%dT%H:%M:%SZ") }
        return self.parse_resp(
            self.call("listIntervals", ixPerson = 1, **args),
            "interval", self.Interval)

    def people(self):
        # I can't work out why the FB api won't give me both normal and delted users
        self.logon_if_required()
        normal =  self.parse_resp(
            self.call("listPeople", fIncludeNormal=1),
            "person", self.Person)
        deleted =  self.parse_resp(
            self.call("listPeople", fIncludeDeleted=1),
            "person", self.Person)
        return normal + deleted

    def cases(self, ids):
        self.logon_if_required()
        return self.parse_resp(
            self.call("search",
                      q=",".join(u"%s" % i for i in set(ids)),
                      cols = "ixBug,ixProject,sProject"),
            "case", self.Case)


class Harvest(API):
    "The Harvest API"

    User = api_node("User", {
        "id" : ("id", int),
        "email" : ("email", unicode),
        })

    DayEntry = api_node("DayEntry", {
        "id" : ("id", int),
        "project" : ("project", unicode),
        "hours" : ("hours", float),
        "notes" : ("notes", unicode),
        "spent_at" : ("spent_at", datestamp),
        })

    Project = api_node("Project", {
        "id" : ("id", int),
        "name" : ("name", unicode)
        })

    DevTask = namedtuple("DevTask", "project_id project_name task_id task_name")

    def __init__(self, url, email, password):
        super(Harvest, self).__init__()
        self.api_url = url
        self.email = email
        self.password = password

    def open(self, url, data=None):
        logger.debug("request url=%s", url)
        headers={
            'Authorization':'Basic '+b64encode('%s:%s' % (self.email, self.password)),
            'Accept':'application/xml',
            'Content-Type':'application/xml',
            'User-Agent':'fogharvest.py',
        }
        request = urllib2.Request(url=url, headers=headers, data=data)
        ret = urllib2.urlopen(request).read()
        logger.debug("response %s", ret)
        return StringIO(ret)

    def daily(self, date):
        url = urlparse.urljoin(self.api_url, date.strftime("/daily/%j/%Y"))
        return self.parse_resp(self.open(url), "day_entry", self.DayEntry)

    def daily_dev_tasks(self, date):
        "The development task for each project"
        url = urlparse.urljoin(self.api_url, date.strftime("/daily/%j/%Y"))
        acc = []
        for proj in ET.parse(self.open(url)).findall(".//projects/project"):
            project_name = proj.find('name').text
            try:
                task = [t for t in proj.findall(".//task")
                        if t.find('name').text.startswith('Development')][0]
                acc.append(self.DevTask(project_id = proj.find('id').text,
                                        project_name = project_name,
                                        task_id = task.find('id').text,
                                        task_name = task.find('name').text))
            except IndexError:
                logging.debug("No task starting 'Development' found in %r", project_name)
        return acc

    def add_daily(self, data, of_user=None):
        "Post a time interval"
        url = urlparse.urljoin(self.api_url, "/daily/add")
        if of_user:
            url = url + "?" + urllib.urlencode({"of_user" : of_user})
        return self.parse_resp(self.open(url, data), "day_entry", self.DayEntry)

    def projects(self):
        url = urlparse.urljoin(self.api_url, "/projects")
        return self.parse_resp(self.open(url), "project", self.Project)

    def people(self):
        url = urlparse.urljoin(self.api_url, "/people")
        return self.parse_resp(self.open(url), "user", self.User)



def idx(l, key="id"):
    "Index - a dict of items in list keyed by id"
    return { getattr(i, key) : i for i in l }


def join(fb, harvest, start=None):
    intervals = fb.intervals(start)
    person_idx = idx(fb.people())
    cases_idx = idx(fb.cases([i.bug_id for i in intervals]))
    projects_idx = { i.project_name : i for i in harvest.daily_dev_tasks(datetime.date.today()) }

    for interval in intervals:
        row = interval._asdict()
        try:
            row.update(person_idx[interval.person_id]._asdict())
            row.update(cases_idx[interval.bug_id]._asdict())
            row.update(projects_idx[row["project_name"]]._asdict())
            yield Storage(row)
        except KeyError, err:
            tb = sys.exc_info()[2]
            try:
                logging.debug("Dropping interval - missing key %r (line %d)", err.args[0], tb.tb_lineno)
            finally:
                del tb

def hours(i):
    "Hours in an interval"
    return ((i.end - i.start).seconds / 60.0 / 60)


# see http://www.getharvest.com/api/time-tracking#create-entry
def harvest_timesheet(rec):
    "Harvest time entry"
    request = ET.Element("request")
    rnotes = ET.SubElement(request, "notes")
    rnotes.text = u"(%s) %s" % (rec.bug_id, rec.title)
    rspent_at = ET.SubElement(request, "spent_at", attrib = {"type":"date"})
    rspent_at.text = rec.start.strftime("%d %b, %Y")
    rhours = ET.SubElement(request, "hours")
    rhours.text= "%4.2f" % hours(rec)
    rtask = ET.SubElement(request, "task_id", attrib = {"type":"integer"})
    rtask.text = rec.task_id
    rproject = ET.SubElement(request, "project_id", attrib = {"type":"integer"})
    rproject.text = rec.project_id
    return request



## command-line handling
def argparser():
    midnight = lambda date: datetime.datetime.combine(date, datetime.time.min)

    class Debug(argparse.Action):
        def __call__(self, parser, namespace, values, option_string=None):
            setattr(namespace, "verbosity", logging.DEBUG)
            setattr(namespace, "logfile", sys.stderr)
            setattr(namespace, "debug", True)
            setattr(namespace, "logformat", "%(name)s %(levelname)s:%(message)s")

    parser = argparse.ArgumentParser(description="")
    parser.add_argument(
        '--config',
        nargs="?",
        default="fogharvest.cfg",
        help='config file',
        type=argparse.FileType(mode="rb"))
    parser.add_argument(
        '--logfile',
        nargs="?",
        default="fogharvest.log",
        help="log file",
        type=argparse.FileType(mode='a'))
    parser.add_argument(
        '-n', '--dry-run',
        action="store_true",
        default = False,
        help = "Don't post data to Harvest")
    parser.add_argument('--user', help="limit processing to a single user (email address)")
    parser.add_argument(
        '--start',
        nargs="?",
        help="date to start at (YYYY-MM-DD)",
        default=midnight(datetime.date.today() - datetime.timedelta(days=1)),
        type=datestamp)
    parser.add_argument(
        '--end',
        nargs="?",
        help="date to end at (YYYY-MM-DD)",
        default=midnight(datetime.date.today()),
        type=datestamp)
    group = parser.add_mutually_exclusive_group(required=False)
    group.add_argument(
        '-v', '--verbosity', default=logging.WARN, choices="DEBUG INFO WARNING ERROR CRITICAL".split())
    group.add_argument('--debug', action=Debug, nargs=0)
    parser.set_defaults(logformat="%(asctime)s %(name)s %(levelname)s:%(message)s")
    return parser


def main(argv=None):
    if not argv:
        argv = sys.argv

    try:
        parser = argparser()
        args = parser.parse_args(argv[1:])
    except Exception, err:
        print >>sys.stderr, err
        print >>sys.stderr, "Cannot parse arguments"
        print >>sys.stderr, "For help use --help"
        return 2

    try:
        logging.basicConfig(level=args.verbosity, stream=args.logfile,
                            format=args.logformat)

        logger.debug("Reading config from %s", args.config)
        cfgparser = RawConfigParser()
        cfgparser.readfp(args.config)

        fb = FB(**dict(cfgparser.items("fogbugz")))
        harvest = Harvest(**dict(cfgparser.items("harvest")))

        logging.info("Starting run start=%s, end=%s", args.start.strftime("%c"), args.end.strftime("%c"))
        records = list(join(fb, harvest, start=args.start))
        logging.info("start with %d intervals", len(records))
        if args.user:
            records = [r for r in records if r.email == args.user]
            logging.info("%d intervals after user filter (%s)", len(records), args.user)

        records = [r for r in records if r.start >= args.start]
        logging.info("%d intervals after start filter (%s)", len(records), args.start.strftime("%c"))
        records = [r for r in records if r.start < args.end]
        logging.info("%d intervals after end filter (%s)", len(records),  args.end.strftime("%c"))

        # if we have records for more than one person, use
        # admin-only endpoints in the Harvest API to submit time for
        # accounts other than the one in the cfg file
        if set(r.email for r in records) != set([cfgparser.get("harvest", "email")]):
            people = idx(harvest.people(), key="email")
            for rec in records:
                try:
                    rec["harvest_user_id"] = people[rec.email].id
                except KeyError:
                    logger.warn("User %r in FB but not in Harvest - dropping record", rec.email)

        logging.info("Processing %d records", len(records))
        for rec in records:
            # submitting a dayentry to Harvest with hours=0 starts a timer.
            if hours(rec) == 0:
                logger.info("Dropping empty interval (email=%s, bug_id=%s)", rec.email, rec.bug_id)
                continue
            logger.info("submitting %s  %s  (%d) %s",
                        rec.email, rec.start.date().strftime("%d %b %Y"), rec.bug_id, rec.title)
            if not args.dry_run:
                # harvest_user_id==None the api will assume we're refurring to the connected user
                resp = harvest.add_daily(ET.tostring(harvest_timesheet(rec)), rec.get("harvest_user_id"))
                logger.info("success: %r", resp)

        logging.info("Ending run")
        return 0
    except Exception, err:
        logger.exception("Terminating on exception")
        print >>sys.stderr, "Error encountered (check log for more details)"
        print >>sys.stderr, err
        return 2



if __name__ == "__main__":
    try:
        __IPYTHON__
    except NameError:
        import sys
        sys.exit(main(sys.argv))



