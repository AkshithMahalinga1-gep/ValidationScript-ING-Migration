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

    # --- Load Excel Sheets ---
    sheets = pd.read_excel(excel_file, sheet_name=None)
    print("Loaded sheets:", sheets.keys())
    srsa_df = sheets["Supplier Risk Assessment Header"]
    form_df = sheets["Form Details"]
    response_df = sheets["Form Response"]

    # --- Initialize Result Log ---
    validation_log = []

    # --- Validation Loop ---
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
            validation_log.append({"ReferenceID": reference_id, "Issue": "Pre Contract SRSA document missing"})
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
                validation_log.append({
                    "ReferenceID": reference_id,
                    "FormID": masterFormId,
                    "Issue": "Form missing in DB"
                })

    # --- Output Result ---
    if validation_log:
        result_df = pd.DataFrame(validation_log)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        input_file_name = os.path.splitext(os.path.basename(excel_file))[0]
        output_file_name = f"{input_file_name}_ValidationResult_{timestamp}.csv"

        # Save the result DataFrame to the constructed file name
        result_df.to_csv(output_file_name, index=False)
        print(f"Validation issues found for {excel_file}. See {output_file_name}")
    else:
        print(f"Validation successful for {excel_file}. All data matched.")