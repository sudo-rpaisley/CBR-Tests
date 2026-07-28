import json
from vehicle_records import create_report, export_report, list_vehicles, save_translation, save_vehicle

def test_vehicle_specific_codes_and_full_report_export(tmp_path):
    db=tmp_path/"vin.sqlite3"; vehicle=save_vehicle(db,"1M8GDM9AXKP042788","Shop van",{"make":"Example","model":"Roadster"})
    assert list_vehicles(db)[0]["name"]=="Shop van"
    save_translation(db,"P0300","Generic misfire"); save_translation(db,"P0300","Example misfire","Example","Roadster")
    report=create_report(db,vehicle["id"],"full","Inspection",notes="Replaced plugs",odometer="100000",codes=["p0300"],work=[{"description":"Replaced plugs"}])
    assert report["codes"][0]["description"]=="Example misfire"
    output=export_report(db,report["id"],tmp_path/"report.json")
    assert json.loads(output.read_text())["work"]==[{"description":"Replaced plugs"}]
