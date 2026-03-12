import os
import requests
from icalendar import Calendar
from datetime import datetime
import pyairtable
import hashlib

TEAMS = [
    {"name": "9U White",   "ics_url": "https://api.team-manager.gc.com/ics-calendar-documents/user/e40c6f49-509e-47d2-a597-4740188fd1ee.ics?teamId=f9cd1378-be84-44e8-9193-eb743180c304&token=6aaec699e5bac2d0b05ba8fbcb03c15b819f4ef602b4ee7d2cda7a8cd110149d"},
    #{"name": "10U Blue",  "ics_url": "https://gc.com/ics/TEAM_B_TOKEN"},
    #{"name": "12U",       "ics_url": "https://gc.com/ics/TEAM_C_TOKEN"},
    # ... add all teams
]

AIRTABLE_TOKEN = os.environ["AIRTABLE_TOKEN"]
AIRTABLE_BASE = "appCCNs65WCh10a9R"
AIRTABLE_TABLE  = "Master Schedule"

def parse_location(location_str):
    """Split 'Meadowbrook Field 3, 123 Main St' into field and address."""
    if not location_str:
        return "", ""
    parts = location_str.split(",", 1)
    field = parts[0].strip()
    address = parts[1].strip() if len(parts) > 1 else ""
    return field, address

def extract_opponent(summary, team_name):
    """Parse 'Team A vs Team B' style summaries."""
    summary = summary or ""
    if " vs " in summary:
        parts = summary.split(" vs ")
        for part in parts:
            if team_name.lower() not in part.lower():
                return part.strip()
    if " @ " in summary:
        parts = summary.split(" @ ")
        if len(parts) > 1:
            return parts[0].strip()
    return ""

def infer_event_type(summary):
    s = (summary or "").lower()
    if "practice" in s: return "Practice"
    if "tournament" in s or "tourney" in s: return "Tournament"
    if "scrimmage" in s: return "Scrimmage"
    if "game" in s or "vs" in s or "@" in s: return "Game"
    return "Other"

def sync_all_teams():
    api = pyairtable.Api(AIRTABLE_TOKEN)
    table = api.table(AIRTABLE_BASE, AIRTABLE_TABLE)
    
    # Build lookup of existing records
    existing = {r["fields"].get("event_id"): r["id"] 
                for r in table.all() if "event_id" in r["fields"]}
    
    for team in TEAMS:
        resp = requests.get(team["ics_url"], timeout=10)
        cal  = Calendar.from_ical(resp.content)
        
        for component in cal.walk():
            if component.name != "VEVENT":
                continue
            
            uid     = str(component.get("UID", ""))
            summary = str(component.get("SUMMARY", ""))
            dtstart = component.get("DTSTART").dt
            dtend   = component.get("DTEND").dt
            loc     = str(component.get("LOCATION", ""))
            
            field, address = parse_location(loc)
            
            # Stable unique ID per team+event
            event_id = hashlib.md5(
                f"{team['name']}-{uid}".encode()
            ).hexdigest()[:12]
            
            record = {
                "event_id":        event_id,
                "team_name":       team["name"],
                "date":            dtstart.strftime("%Y-%m-%d"),
                "start_time":      dtstart.strftime("%H:%M"),
                "end_time":        dtend.strftime("%H:%M"),
                "event_type":      infer_event_type(summary),
                "opponent":        extract_opponent(summary, team["name"]),
                "location":        address,
                "field":           field,
                "gamechanger_id":  uid,
                "last_updated":    datetime.now().isoformat(),
            }
            
            if event_id in existing:
                table.update(existing[event_id], record)
            else:
                table.create(record)
    
    print(f"Sync complete: {len(TEAMS)} teams processed")

if __name__ == "__main__":
    sync_all_teams()
