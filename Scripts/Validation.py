import pandas as pd
from pymongo import MongoClient
from tkinter import Tk
from tkinter.filedialog import askopenfilenames
import os
from datetime import datetime

# --- Simple File Upload Dialog ---
Tk().withdraw()  # Prevent full GUI window
print("Please select the migration Excel files...")
EXCEL_FILES = askopenfilenames(filetypes=[("Excel Files", "*.xlsx")])

if not EXCEL_FILES:
    print("No files selected. Exiting.")
    exit()

# --- Configuration ---
MONGO_URI = "mongodb+srv://uatleoeutenantdocteamsro:XnfQ6pixSookjshO@uat-eu-leo.mwfvc.mongodb.net/?ssl=true&authSource=admin&retryWrites=true&readPreference=secondaryPreferred&w=majority&wtimeoutMS=5000&readConcernLevel=majority&retryReads=true&appName=docteamtprmro"
DB_NAME = "uatdomainmodeling"
SRSA_COLLECTION = "riskAssessment_1664901704"
FORM_COLLECTION = "form_1663277990"
RESPONSE_COLLECTION = "responses"

# --- Connect to MongoDB ---
client = MongoClient(MONGO_URI)
db = client[DB_NAME]

# --- Process Each File ---
for excel_file in EXCEL_FILES:
    print(f"Processing file: {excel_file}")

    sheets = pd.read_excel(excel_file, sheet_name=None)
    sheets = {sheet_name: sheet_data for i, (sheet_name, sheet_data) in enumerate(sheets.items()) if i > 0}    
    print("Loaded sheets:", sheets.keys())
    srsa_df = sheets["Supplier Risk Assessment Header"]
    form_df = sheets["Form Details"]
    response_df = sheets["Form Response"]

    validation_logs = {
        "Supplier Risk Assessment Header": [],
        "Form Details": [],
        "Form Response": [],
    }

    for _, srsa in srsa_df.iterrows():
        print(f"Validating Reference ID: {srsa['Reference ID*']}")
        reference_id = srsa["Reference ID*"]
        contract_id = srsa["Contract Id"]
        srsa_doc = db[SRSA_COLLECTION].find_one({"revisedContractNumber": contract_id, "isDeleted": False}, {"internalDocumentId": 1, "documentNumber": 1})
        print(f"SRSA Document: {srsa_doc}")
        preContractSrsa_internalDocumentId = ""
        if srsa_doc:
            documentNumber = srsa_doc["documentNumber"]
            srsa_pre_contract_doc = db[SRSA_COLLECTION].find_one({"documentNumber": documentNumber, "dueDiligencePhase": "Pre Contract"}, {"internalDocumentId": 1})
            if srsa_pre_contract_doc:
                preContractSrsa_internalDocumentId = srsa_pre_contract_doc["internalDocumentId"]

        if preContractSrsa_internalDocumentId == "":
            validation_logs["Supplier Risk Assessment Header"].append({
                "ReferenceID": reference_id,
                "Issue": "Pre Contract SRSA document missing"
            })
            continue

        expected_forms = form_df[form_df["Reference ID*"] == reference_id]

        master_form_ids = expected_forms["Master Form ID*"].tolist()

        # Fetch all forms from the database in one query
        db_forms = list(db[FORM_COLLECTION].find(
            {
                "sourceFormDocumentNumber": {"$in": master_form_ids},
                "supplierRSAId": preContractSrsa_internalDocumentId,
                "isDeleted": False
            },
            {"sourceFormDocumentNumber": 1, "internalDocumentId": 1}
        ))

        # Create a set of found Master Form IDs for quick lookup
        found_form_ids = {form["sourceFormDocumentNumber"] for form in db_forms}

        for _, form in expected_forms.iterrows():
            masterFormId = form.get("Master Form ID*")
            print(f"Validating Form ID: {masterFormId} form Found is {form}")
            if masterFormId not in found_form_ids:
                validation_logs["Form Details"].append({
                    "ReferenceID": reference_id,
                    "FormID": masterFormId,
                    "Issue": "Form missing in DB"
                })

    # --- Output Result ---
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    input_file_name = os.path.splitext(os.path.basename(excel_file))[0]
    output_file_name = f"{input_file_name}_ValidationResult_{timestamp}.xlsx"

    with pd.ExcelWriter(output_file_name, engine='xlsxwriter') as writer:
        # Write validation logs to corresponding sheets
        for sheet_name, sheet_data in sheets.items():
            # Write original data to the sheet
            if validation_logs[sheet_name]:
                validation_df = pd.DataFrame(validation_logs[sheet_name])
                validation_df.to_excel(writer, sheet_name=f"{sheet_name}_Validation", index=False)

        print(f"Validation results saved to {output_file_name}")