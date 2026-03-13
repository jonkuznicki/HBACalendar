import os
import requests
from icalendar import Calendar
from datetime import datetime, date, timedelta
import pyairtable
import hashlib

TEAMS = [
    {"name": "9U White", "ics_url": "https://api.team-manager.gc.com/ics-calendar-documents/user/e40c6f49-509e-47d2-a597-4740188fd1ee.ics?teamId=f9cd1378-be84-44e8-9193-eb743180c304&token=6aaec699e5bac2d0b05ba8fbcb03c15b819f4ef602b4ee7d2cda7a8cd110149d"},
    # add more teams here
]

AIRTABLE_TOKEN = os.environ["AIRTABLE_TOKEN"]
AIRTABLE_BASE  = "appCCNs65WCh10a9R"
AIRTABLE_TABLE = "Master Schedule"

def clean_location(loc_str):
    """Return the full raw location string from GameChanger, cleaned up."""
    if not loc_str:
        return ""
    # GameChanger sometimes puts newlines in location
    return " ".join(loc_str.strip().splitlines())

def extract_opponent(summary, team_name):
    """Extract opponent from 'Team A vs Team B' or 'Team A @ Team B'."""
    summary = summary or ""
    team_lower = team_name.lower()

    if " vs " in summary:
        parts = summary.split(" vs ")
        for part in parts:
            cleaned = part.strip()
            if team_lower not in cleaned.lower():
                return cleaned
    if " @ " in summary:
        parts = summary.split(" @ ")
        # Format is typically "Away @ Home" — opponent is whoever isn't us
        for part in parts:
            cleaned = part.strip()
            if team_lower not in cleaned.lower():
                return cleaned
    return ""

def infer_event_type(summary, gc_type=None):
    """Infer event type from summary text."""
    s = (summary or "").lower()
    if "practice" in s:       return "Practice"
    if "tournament" in s or "tourney" in s: return "Tournament"
    if "scrimmage" in s:      return "Scrimmage"
    if " vs " in s or " @ " in s: return "Game"
    return "Other"

def get_description(component):
    """Extract notes/description from ICS component."""
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

            uid     = str(component.get("UID", ""))
            summary = str(component.get("SUMMARY", ""))
            loc     = str(component.get("LOCATION", ""))
            desc    = get_description(component)
            dtstart = component.get("DTSTART").dt
            dtend   = component.get("DTEND").dt

            event_type   = infer_event_type(summary)
            opponent     = extract_opponent(summary, team["name"])
            location     = clean_location(loc)

            # Build notes — include description for Practice and Other
            notes = ""
            if event_type in ("Practice", "Other") and desc:
                notes = desc
            elif event_type == "Tournament" and desc:
                notes = desc

            # Handle all-day multi-day events (tournaments etc.)
            # These come in as date objects, not datetime objects
            is_allday = isinstance(dtstart, date) and not isinstance(dtstart, datetime)

            if is_allday:
                # Create one record per day of the event
                current_day = dtstart
                # ICS all-day end date is exclusive (day after last day)
                last_day = dtend - timedelta(days=1)
                day_num  = 1
                total_days = (dtend - dtstart).days

                while current_day <= last_day:
                    day_label = f" (Day {day_num} of {total_days})" if total_days > 1 else ""
                    event_id  = hashlib.md5(
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

    print(f"Sync complete: {len(TEAMS)} teams processed")

if __name__ == "__main__":
    sync_all_teams()
