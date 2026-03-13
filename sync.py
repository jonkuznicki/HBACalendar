import os
import requests
from icalendar import Calendar
from datetime import datetime, date, timedelta
import pyairtable
import hashlib

AIRTABLE_TOKEN = os.environ["AIRTABLE_TOKEN"]
AIRTABLE_BASE  = "appCCNs65WCh10a9R"
AIRTABLE_TABLE = "Master Schedule"
TEAMS_TABLE    = "Teams"

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

def infer_event_type(summary, description=""):
    s = (summary or "").lower()
    d = (description or "").lower()
    if "practice" in s: return "Practice"
    if "tournament" in s or "tourney" in s or "classic" in s or "bash" in s: return "Tournament"
    if "tournament" in d or "tourney" in d: return "Tournament"
    if "scrimmage" in s: return "Scrimmage"
    if " vs " in s or " @ " in s: return "Game"
    return "Other"

def get_description(component):
    desc = component.get("DESCRIPTION")
    if not desc:
        return ""
    return str(desc).strip()

def load_teams(api):
    table = api.table(AIRTABLE_BASE, TEAMS_TABLE)
    teams = []
    for r in table.all():
        f = r["fields"]
        if f.get("active") and f.get("ics_url"):
            teams.append({
                "name":    f.get("team_name", ""),
                "gc_name": f.get("gc_name", ""),
                "ics_url": f.get("ics_url", ""),
            })
    print(f"Loaded {len(teams)} active teams from Airtable")
    return teams

def sync_all_teams():
    api   = pyairtable.Api(AIRTABLE_TOKEN)
    table = api.table(AIRTABLE_BASE, AIRTABLE_TABLE)

    # Load teams from config table
    teams = load_teams(api)
    if not teams:
        print("No active teams found in Teams table — exiting")
        return

    existing = {r["fields"].get("event_id"): r["id"]
                for r in table.all() if "event_id" in r["fields"]}

    synced_ids = set()

    for team in teams:
        print(f"Fetching: {team['name']}")
        resp = requests.get(team["ics_url"], timeout=10)
        resp.raise_for_status()
        cal = Calendar.from_ical(resp.content)

        for component in cal.walk():
            if component.name != "VEVENT": continue
            uid        = str(component.get("UID", ""))
            summary    = str(component.get("SUMMARY", ""))
            loc        = str(component.get("LOCATION", ""))
            desc       = get_description(component)
            dtstart    = component.get("DTSTART").dt
            dtend      = component.get("DTEND").dt
            event_type = infer_event_type(summary, desc)
            opponent   = extract_opponent(summary, team["name"], team.get("gc_name", ""))
            location   = clean_location(loc)
            notes      = desc if event_type in ("Practice", "Tournament", "Other") and desc else ""
            is_allday  = isinstance(dtstart, date) and not isinstance(dtstart, datetime)
            home_away  = ("Away" if " @ " in summary else "Home") if event_type == "Game" else ""

            if is_allday:
                current_day = dtstart
                last_day    = dtend - timedelta(days=1)
                total_days  = (dtend - dtstart).days
                day_num     = 1
                while current_day <= last_day:
                    day_label = f" (Day {day_num} of {total_days})" if total_days > 1 else ""
                    event_id = hashlib.md5(f"{team['name']}-{uid}-{current_day}".encode()).hexdigest()[:12]
                    synced_ids.add(event_id)
                    record = {
                        "event_id":      event_id,
                        "team_name":     team["name"],
                        "date":          current_day.strftime("%Y-%m-%d"),
                        "start_time":    "All Day",
                        "end_time":      "",
                        "event_type":    event_type,
                        "event_title":   summary + day_label,
                        "opponent":      opponent,
                        "location":      location,
                        "home_away":     home_away,
                        "gamechanger_id": uid,
                        "notes":         notes,
                        "last_updated":  datetime.now().isoformat(),
                    }
                    if event_id in existing: table.update(existing[event_id], record)
                    else: table.create(record)
                    current_day += timedelta(days=1)
                    day_num += 1
            else:
                event_id = hashlib.md5(f"{team['name']}-{uid}".encode()).hexdigest()[:12]
                synced_ids.add(event_id)
                record = {
                    "event_id":      event_id,
                    "team_name":     team["name"],
                    "date":          dtstart.strftime("%Y-%m-%d"),
                    "start_time":    dtstart.strftime("%H:%M"),
                    "end_time":      dtend.strftime("%H:%M"),
                    "event_type":    event_type,
                    "event_title":   summary,
                    "opponent":      opponent,
                    "location":      location,
                    "home_away":     home_away,
                    "gamechanger_id": uid,
                    "notes":         notes,
                    "last_updated":  datetime.now().isoformat(),
                }
                if event_id in existing: table.update(existing[event_id], record)
                else: table.create(record)
        print(f"  Done: {team['name']}")

    # Delete any Airtable records that no longer exist in GameChanger
    stale_ids = [airtable_id for event_id, airtable_id in existing.items()
                 if event_id not in synced_ids]
    if stale_ids:
        print(f"Deleting {len(stale_ids)} stale records...")
        for i in range(0, len(stale_ids), 10):
            batch = stale_ids[i:i+10]
            for rid in batch:
                table.delete(rid)
        print(f"  Deleted {len(stale_ids)} records")
    else:
        print("No stale records to delete")

    print(f"Sync complete: {len(teams)} teams processed")

if __name__ == "__main__":
    sync_all_teams()
