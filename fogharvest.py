#!/usr/bin/env python
"""
Forward time logged against tickets in FogBugz to Harvest.
"""

import urllib, urlparse, urllib2
import datetime
import xml.etree.ElementTree as ET
import logging
from ConfigParser import RawConfigParser
from base64 import b64encode
from collections import namedtuple
from StringIO import StringIO
from operator import attrgetter
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
    def __init__(self, mapping):
        self.__dict__.update(mapping)

    def __repr__(self):
        return "Storage(%r)" % self.__dict__

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

    def intervals(self):
        self.logon_if_required()
        return self.parse_resp(
            self.call("listIntervals", ixPerson = 1),
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

def join(fb, harvest):
    idx = lambda l: { i.id : i for i in l }
    intervals = fb.intervals()
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
            logging.debug("Missing key", exc_info=True)


def hours(i):
    "Hours in an interval"
    return ((i.end - i.start).seconds / 60.0 / 60)

def html_timesheets(itr):
    from html import HTML
    h = HTML()
    for day, dintervals in sgroup(itr, key=lambda i: i.start.date()):
        h.h1(day.strftime("%d %b %Y"))
        for person, pintervals in sgroup(dintervals, key=attrgetter("email")):
            h.h2(person)
            for project, intervals in sgroup(pintervals, key=lambda i: i.project_name):
                h.h3(project)
                with h.table:
                    htotal = 0
                    for bug_title, gintervals in sgroup(intervals, key=lambda i: i.title):
                        gintervals = list(gintervals)
                        with h.tr:
                            h.td(bug_title)
                            h.td("%4.2f" % sum(hours(i) for i in gintervals))
                        htotal += sum(hours(i) for i in gintervals)
                    with h.tr:
                        h.td("total")
                        h.td("%4.2f" % htotal)
    return str(h)


def harvest_timesheets(itr):
    def notes(timeslices):
        return "\n".join("%s (%4.2f h)" % (case, sum(hours(i) for i in intervals))
                         for case, intervals in timeslices.items())

    def timesheets(itr):
        for day, dintervals in sgroup(itr, key=lambda i: i.start.date()):
            for person, pintervals in sgroup(dintervals, key=attrgetter("email")):
                for project, intervals in sgroup(pintervals, key=lambda i: i.project_name):
                    for interval in intervals:
                        yield day, person, project, interval

    for day, person, project, interval in timesheets(itr):
        request = ET.Element("request")
        rnotes = ET.SubElement(request, "notes")
        rnotes.text = u"(%s) %s" % (interval.bug_id, interval.title)
        rspent_at = ET.SubElement(request, "spent_at", attrib = {"type":"date"})
        rspent_at.text = day.strftime("%d %b, %Y")
        rhours = ET.SubElement(request, "hours")
        rhours.text= "%4.2f" % hours(interval)
        rtask = ET.SubElement(request, "task_id", attrib = {"type":"integer"})
        rtask.text = interval.task_id
        rproject = ET.SubElement(request, "project_id", attrib = {"type":"integer"})
        rproject.text = interval.project_id
        yield request

def harvest_timesheet(interval):
    request = ET.Element("request")
    rnotes = ET.SubElement(request, "notes")
    rnotes.text = u"(%s) %s" % (interval.bug_id, interval.title)
    rspent_at = ET.SubElement(request, "spent_at", attrib = {"type":"date"})
    rspent_at.text = interval.start.strftime("%d %b, %Y")
    rhours = ET.SubElement(request, "hours")
    rhours.text= "%4.2f" % hours(interval)
    rtask = ET.SubElement(request, "task_id", attrib = {"type":"integer"})
    rtask.text = interval.task_id
    rproject = ET.SubElement(request, "project_id", attrib = {"type":"integer"})
    rproject.text = interval.project_id
    return request



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

    def __init__(self,url,email,password):
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

    def add_daily(self, data):
        "Post a time interval"
        url = urlparse.urljoin(self.api_url, "/daily/add")
        return self.parse_resp(self.open(url, data), "day_entry", self.DayEntry)

    def projects(self):
        url = urlparse.urljoin(self.api_url, "/projects")
        return self.parse_resp(self.open(url), "project", self.Project)

    def people(self):
        url = urlparse.urljoin(self.api_url, "/people")
        return self.parse_resp(self.open(url), "user", self.User)

## command-line handling
def argparser():
    import argparse
    class Debug(argparse.Action):
        def __call__(self, parser, namespace, values, option_string=None):
            setattr(namespace, "verbosity", logging.DEBUG)
            setattr(namespace, "logfile", sys.stderr)
            setattr(namespace, "debug", True)

    parser = argparse.ArgumentParser(description="")
    parser.add_argument(
        '--config',
        default="fogharvest.cfg",
        help='config file')
    parser.add_argument(
        '--logfile',
        default="fogharvest.log",
        help="log file",
        type=argparse.FileType(mode='a'))
    parser.add_argument(
        '--dry-run',
        action="store_true",
        default = False,
        help = "Don't post data to Harvest")
    parser.add_argument('--user', help="limit processing to a single user (email address)")
    parser.add_argument('--start', help="date to start at (YYYY-MM-DD)", type=datestamp)
    parser.add_argument('--end', help="date to end at (YYYY-MM-DD)", type=datestamp)
    group = parser.add_mutually_exclusive_group(required=False)
    group.add_argument(
        '-v', '--verbosity', default=logging.WARN, choices="DEBUG INFO WARNING ERROR CRITICAL".split())
    group.add_argument('--debug', action=Debug, nargs=0)
    return parser


def main(argv=None):
    if not argv:
        argv = sys.argv
    parser = argparser()
    args = parser.parse_args(argv[1:])
    logging.basicConfig(level=args.verbosity, stream=args.logfile)

    logger.debug("Reading config from %s", args.config)
    cfgparser = RawConfigParser()
    cfgparser.read(args.config)

    fb = FB(**dict(cfgparser.items("fogbugz")))
    harvest = Harvest(**dict(cfgparser.items("harvest")))

    records = list(join(fb, harvest))
    logging.info("start with %d intervals", len(records))
    if args.user:
        records = [r for r in records if r.email == args.user]
        logging.info("%d intervals after user filter (%s)", len(records), args.user)
    if args.start:
        records = [r for r in records if r.start >= args.start]
        logging.info("%d intervals after start filter (%s)", len(records), args.start.strftime("%c"))
    if args.end:
        records = [r for r in records if r.start < args.end]
        logging.info("%d intervals after end filter (%s)", len(records),  args.end.strftime("%c"))

    logging.info("Processing %d records", len(records))
    for rec in records:
        logger.info("rec bug=%s title=%s email=%s", rec.bug_id, rec.title, rec.email)
        if not args.dry_run:
            resp = harvest.add_daily(ET.tostring(harvest_timesheet(rec)))
            logger.debug(resp)

    return 0


if __name__ == "__main__":
    try:
        __IPYTHON__
    except NameError:
        import sys
        sys.exit(main(sys.argv))



