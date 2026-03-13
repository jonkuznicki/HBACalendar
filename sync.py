import os
import requests
from icalendar import Calendar
from datetime import datetime, date, timedelta
import pyairtable
import hashlib

TEAMS = [
    {
        "name": "9U White",
        "gc_name": "Hudson Explorers White 9U",
        "ics_url": "https://api.team-manager.gc.com/ics-calendar-documents/user/e40c6f49-509e-47d2-a597-4740188fd1ee.ics?teamId=f9cd1378-be84-44e8-9193-eb743180c304&token=6aaec699e5bac2d0b05ba8fbcb03c15b819f4ef602b4ee7d2cda7a8cd110149d"
    },
    {
        "name": "9U Blue",
        "gc_name": "Hudson Explorers Blue 9U",
        "ics_url": "https://api.team-manager.gc.com/ics-calendar-documents/user/e40c6f49-509e-47d2-a597-4740188fd1ee.ics?teamId=44a9491e-2152-466f-b914-b97b547294dc&token=fbe979fba7233a284f7dc1f02539f0dc4c0f00d9b7b6d7bbe56d4b988eda2f29"
    },
]

AIRTABLE_TOKEN = os.environ["AIRTABLE_TOKEN"]
AIRTABLE_BASE  = "appCCNs65WCh10a9R"
AIRTABLE_TABLE = "Master Schedule"

def clean_location(loc_str):
    if not loc_str:
        return ""
    return " ".join(loc_str.strip().splitlines())

def extract_opponent(summary, team_name, gc_team_name=""):
    summary = summary or ""
    names_to_exclude = [n.lower() for n in [team_name, gc_team_name] if n]

    if " vs " in summary:
        parts = summary.split(" vs ")
        for part in parts:
            cleaned = part.strip()
            if not any(n in cleaned.lower() for n in names_to_exclude):
                return cleaned

    if " @ " in summary:
        parts = summary.split(" @ ")
        for part in parts:
            cleaned = part.strip()
            if not any(n in cleaned.lower() for n in names_to_exclude):
                return cleaned

    return ""

def infer_event_type(summary):
    s = (summary or "").lower()
    if "practice" in s:
        return "Practice"
    if "tournament" in s or "tourney" in s:
        return "Tournament"
    if "scrimmage" in s:
        return "Scrimmage"
    if " vs " in s or " @ " in s:
        return "Game"
    return "Other"

def get_description(component):
    desc = component.get("DESCRIPTION")
    if not desc:
        return ""
    return str(desc).strip()

def sync_all_teams():
    api   = pyairtable.Api(AIRTABLE_TOKEN)
    table = api.table(AIRTABLE_BASE, AIRTABLE_TABLE)

    # Build lookup of existing records by event_id
    existing = {r["fields"].get("event_id"): r["id"]
                for r in table.all() if "event_id" in r["fields"]}

    for team in TEAMS:
        print(f"Fetching: {team['name']}")
        resp = requests.get(team["ics_url"], timeout=10)
        resp.raise_for_status()
        cal = Calendar.from_ical(resp.content)

        for component in cal.walk():
            if component.name != "VEVENT":
                continue

            uid        = str(component.get("UID", ""))
            summary    = str(component.get("SUMMARY", ""))
            loc        = str(component.get("LOCATION", ""))
            desc       = get_description(component)
            dtstart    = component.get("DTSTART").dt
            dtend      = component.get("DTEND").dt

            event_type = infer_event_type(summary)
            opponent   = extract_opponent(summary, team["name"], team.get("gc_name", ""))
            location   = clean_location(loc)

            # Include notes for Practice, Tournament, and Other
            notes = desc if event_type in ("Practice", "Tournament", "Other") and desc else ""

            # Detect all-day events (date object, not datetime)
            is_allday = isinstance(dtstart, date) and not isinstance(dtstart, datetime)

            if is_allday:
                # Expand multi-day events into one record per day
                current_day = dtstart
                last_day    = dtend - timedelta(days=1)  # ICS end date is exclusive
                total_days  = (dtend - dtstart).days
                day_num     = 1

                while current_day <= last_day:
                    day_label = f" (Day {day_num} of {total_days})" if total_days > 1 else ""

                    event_id = hashlib.md5(
                        f"{team['name']}-{uid}-{current_day}".encode()
                    ).hexdigest()[:12]

                    record = {
                        "event_id":       event_id,
                        "team_name":      team["name"],
                        "date":           current_day.strftime("%Y-%m-%d"),
                        "start_time":     "All Day",
                        "end_time":       "",
                        "event_type":     event_type,
                        "event_title":    summary + day_label,
                        "opponent":       opponent,
                        "location":       location,
                        "gamechanger_id": uid,
                        "notes":          notes,
                        "last_updated":   datetime.now().isoformat(),
                    }

                    if event_id in existing:
                        table.update(existing[event_id], record)
                    else:
                        table.create(record)

                    current_day += timedelta(days=1)
                    day_num     += 1

            else:
                # Normal timed event
                event_id = hashlib.md5(
                    f"{team['name']}-{uid}".encode()
                ).hexdigest()[:12]

                record = {
                    "event_id":       event_id,
                    "team_name":      team["name"],
                    "date":           dtstart.strftime("%Y-%m-%d"),
                    "start_time":     dtstart.strftime("%H:%M"),
                    "end_time":       dtend.strftime("%H:%M"),
                    "event_type":     event_type,
                    "event_title":    summary,
                    "opponent":       opponent,
                    "location":       location,
                    "gamechanger_id": uid,
                    "notes":          notes,
                    "last_updated":   datetime.now().isoformat(),
                }

                if event_id in existing:
                    table.update(existing[event_id], record)
                else:
                    table.create(record)

        print(f"  Done: {team['name']}")

    print(f"Sync complete: {len(TEAMS)} teams processed")

if __name__ == "__main__":
    sync_all_teams()
