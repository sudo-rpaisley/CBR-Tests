from code_database_import import import_code_database
from vehicle_records import create_report, save_vehicle

def test_csv_code_database_import_and_matching(tmp_path):
    source=tmp_path/"codes.csv"; source.write_text("code,description,make,model\nP0300,Vehicle misfire,Example,Roadster\n")
    database=tmp_path/"vin.sqlite3"; vehicle=save_vehicle(database,"1M8GDM9AXKP042788","",{"make":"Example","model":"Roadster"})
    assert import_code_database(database,source)==1
    report=create_report(database,vehicle["id"],"diagnostic","Codes",codes=["P0300"])
    assert report["codes"][0]["description"]=="Vehicle misfire"
