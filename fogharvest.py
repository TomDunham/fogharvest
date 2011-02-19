import datetime
from collections import namedtuple

def timestamp(ts):
    return datetime.datetime.strptime(ts, "%Y-%m-%dT%H:%M:%SZ")

def boolstr(bs):
    return bs.lower == u"true"

def parse_person(el):
    emap = {
        u"ixPerson" : ("id", int),
        u"sFullName" : ("full_name", unicode),
        u"sEmail" : ("email", unicode),
        u"dtLastActivity" : ("last_activity", timestamp)
        }

    Person = namedtuple("Person", " ".join(k[0] for k in emap.values()))

    vals = { attr : conv(el.find(tag).text.strip()) for tag, (attr, conv) in emap.items() }

    return Person(**vals)


def parse_interval(el):
    emap = {
        u"ixInterval" : ("id", int),
        u"ixPerson" : ("person_id", int),
        u"ixBug" : ("bug_id", int),
        u"dtStart" : ("start", timestamp),
        u"dtEnd" : ("end", timestamp),
        u"sTitle" : ("title", unicode),
        u"fDeleted" : ("deleted", boolstr)
        }

    Interval = namedtuple("Interval", " ".join(k[0] for k in emap.values()))

    vals = { attr : conv(el.find(tag).text.strip()) for tag, (attr, conv) in emap.items() }

    return Interval(**vals)




