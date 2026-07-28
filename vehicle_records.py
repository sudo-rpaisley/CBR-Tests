"""Local saved-vehicle, code-translation, and service-report storage."""
import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

def _now(): return datetime.now(timezone.utc).isoformat()

def ensure_schema(db):
    db.executescript("""
    CREATE TABLE IF NOT EXISTS saved_vehicles(id INTEGER PRIMARY KEY,vin TEXT UNIQUE NOT NULL,name TEXT NOT NULL DEFAULT '',details_json TEXT NOT NULL,created_at TEXT NOT NULL);
    CREATE TABLE IF NOT EXISTS dtc_translations(id INTEGER PRIMARY KEY,code TEXT NOT NULL,make TEXT NOT NULL DEFAULT '',model TEXT NOT NULL DEFAULT '',description TEXT NOT NULL,UNIQUE(code,make,model));
    CREATE TABLE IF NOT EXISTS vehicle_reports(id INTEGER PRIMARY KEY,vehicle_id INTEGER NOT NULL REFERENCES saved_vehicles(id),report_type TEXT NOT NULL,title TEXT NOT NULL,notes TEXT NOT NULL DEFAULT '',odometer TEXT NOT NULL DEFAULT '',codes_json TEXT NOT NULL,work_json TEXT NOT NULL,created_at TEXT NOT NULL);
    CREATE TABLE IF NOT EXISTS diagnostic_scans(id INTEGER PRIMARY KEY,vehicle_id INTEGER NOT NULL REFERENCES saved_vehicles(id),adapter TEXT NOT NULL DEFAULT '',stored_json TEXT NOT NULL,pending_json TEXT NOT NULL,permanent_json TEXT NOT NULL,created_at TEXT NOT NULL);
    """)

def save_vehicle(database, vin, name="", details=None):
    with sqlite3.connect(database) as db:
        ensure_schema(db); db.execute("INSERT INTO saved_vehicles(vin,name,details_json,created_at) VALUES(?,?,?,?) ON CONFLICT(vin) DO UPDATE SET name=excluded.name,details_json=excluded.details_json",(vin,name.strip(),json.dumps(details or {}),_now()))
        r=db.execute("SELECT id,vin,name,details_json,created_at FROM saved_vehicles WHERE vin=?",(vin,)).fetchone()
    return {"id":r[0],"vin":r[1],"name":r[2],"details":json.loads(r[3]),"created_at":r[4]}

def list_vehicles(database):
    with sqlite3.connect(database) as db:
        ensure_schema(db); rows=db.execute("SELECT id,vin,name,details_json,created_at FROM saved_vehicles ORDER BY name,vin").fetchall()
    return [{"id":r[0],"vin":r[1],"name":r[2],"details":json.loads(r[3]),"created_at":r[4]} for r in rows]

def save_translation(database, code, description, make="", model=""):
    code,make,model=code.strip().upper(),make.strip().upper(),model.strip().upper()
    if not code or not description.strip(): raise ValueError("A code and description are required.")
    with sqlite3.connect(database) as db:
        ensure_schema(db); db.execute("INSERT INTO dtc_translations(code,make,model,description) VALUES(?,?,?,?) ON CONFLICT(code,make,model) DO UPDATE SET description=excluded.description",(code,make,model,description.strip()))
    return {"code":code,"make":make,"model":model,"description":description.strip()}

def _translate(db,codes,vehicle):
    result=[]; make=str(vehicle.get("make","")).upper(); model=str(vehicle.get("model","")).upper()
    for raw in codes:
        code=raw.strip().upper(); row=db.execute("SELECT description,make,model FROM dtc_translations WHERE code=? AND (make='' OR make=?) AND (model='' OR model=?) ORDER BY (make!='')+(model!='') DESC LIMIT 1",(code,make,model)).fetchone()
        result.append({"code":code,"description":row[0] if row else "","matched_make":row[1] if row else "","matched_model":row[2] if row else ""})
    return result

def create_report(database,vehicle_id,report_type,title,notes="",odometer="",codes=None,work=None):
    if report_type not in ("diagnostic","work","full"): raise ValueError("Report type must be diagnostic, work, or full.")
    with sqlite3.connect(database) as db:
        ensure_schema(db); vehicle=db.execute("SELECT details_json FROM saved_vehicles WHERE id=?",(vehicle_id,)).fetchone()
        if not vehicle: raise ValueError("Saved vehicle not found.")
        translated=_translate(db,codes or [],json.loads(vehicle[0])); cur=db.execute("INSERT INTO vehicle_reports(vehicle_id,report_type,title,notes,odometer,codes_json,work_json,created_at) VALUES(?,?,?,?,?,?,?,?)",(vehicle_id,report_type,title.strip() or "Vehicle report",notes,odometer,json.dumps(translated),json.dumps(work or []),_now())); report_id=cur.lastrowid
    return get_report(database,report_id)

def get_report(database,report_id):
    with sqlite3.connect(database) as db:
        ensure_schema(db); r=db.execute("SELECT r.id,r.report_type,r.title,r.notes,r.odometer,r.codes_json,r.work_json,r.created_at,v.id,v.vin,v.name,v.details_json FROM vehicle_reports r JOIN saved_vehicles v ON v.id=r.vehicle_id WHERE r.id=?",(report_id,)).fetchone()
    if not r: raise ValueError("Report not found.")
    return {"id":r[0],"type":r[1],"title":r[2],"notes":r[3],"odometer":r[4],"codes":json.loads(r[5]),"work":json.loads(r[6]),"created_at":r[7],"vehicle":{"id":r[8],"vin":r[9],"name":r[10],"details":json.loads(r[11])}}

def export_report(database,report_id,destination):
    destination=Path(destination); destination.parent.mkdir(parents=True,exist_ok=True); report=get_report(database,report_id)
    if destination.suffix.lower()==".pdf":
        from reportlab.lib.pagesizes import letter
        from reportlab.pdfgen.canvas import Canvas
        canvas=Canvas(str(destination),pagesize=letter); width,height=letter; y=height-54
        lines=[report["title"],f"Vehicle: {report['vehicle']['name']} ({report['vehicle']['vin']})",f"Type: {report['type']}   Created: {report['created_at']}",f"Odometer: {report['odometer']}","", "Diagnostic codes:"]
        lines += [f"{item['code']}: {item['description'] or 'No translation'}" for item in report["codes"]]
        lines += ["", "Work performed:"]+[str(item.get("description",item)) for item in report["work"]]+["", "Notes:", report["notes"]]
        for line in lines:
            for part in [line[i:i+95] for i in range(0,max(1,len(line)),95)]:
                if y<54: canvas.showPage(); y=height-54
                canvas.drawString(54,y,part); y-=16
        canvas.save()
    else: destination.write_text(json.dumps(report,indent=2,sort_keys=True),encoding="utf-8")
    return destination

def save_diagnostic_scan(database,vehicle_id,diagnostics,adapter=""):
    with sqlite3.connect(database) as db:
        ensure_schema(db)
        if not db.execute("SELECT 1 FROM saved_vehicles WHERE id=?",(vehicle_id,)).fetchone(): raise ValueError("Saved vehicle not found.")
        cursor=db.execute("INSERT INTO diagnostic_scans(vehicle_id,adapter,stored_json,pending_json,permanent_json,created_at) VALUES(?,?,?,?,?,?)",(vehicle_id,adapter,json.dumps(diagnostics.get("stored",[])),json.dumps(diagnostics.get("pending",[])),json.dumps(diagnostics.get("permanent",[])),_now()))
        scan_id=cursor.lastrowid
        row=db.execute("SELECT created_at FROM diagnostic_scans WHERE id=?",(scan_id,)).fetchone()
    return {"id":scan_id,"vehicle_id":vehicle_id,"adapter":adapter,"stored":diagnostics.get("stored",[]),"pending":diagnostics.get("pending",[]),"permanent":diagnostics.get("permanent",[]),"created_at":row[0]}

def list_diagnostic_scans(database,vehicle_id):
    with sqlite3.connect(database) as db:
        ensure_schema(db); rows=db.execute("SELECT id,adapter,stored_json,pending_json,permanent_json,created_at FROM diagnostic_scans WHERE vehicle_id=? ORDER BY id DESC",(vehicle_id,)).fetchall()
    return [{"id":r[0],"vehicle_id":vehicle_id,"adapter":r[1],"stored":json.loads(r[2]),"pending":json.loads(r[3]),"permanent":json.loads(r[4]),"created_at":r[5]} for r in rows]
