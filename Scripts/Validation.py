import pandas as pd
from pymongo import MongoClient
from tkinter import Tk
from tkinter.filedialog import askopenfilename
import os
from datetime import datetime
# --- Simple File Upload Dialog ---
Tk().withdraw()  # Prevent full GUI window
print("Please select the migration Excel file...")
EXCEL_FILE = askopenfilename(filetypes=[("Excel Files", "*.xlsx")])

if not EXCEL_FILE:
    print("No file selected. Exiting.")
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

# --- Load Excel Sheets ---
sheets = pd.read_excel(EXCEL_FILE, sheet_name=None)
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
    srsa_doc = db[SRSA_COLLECTION].find_one({"revisedContractNumber": contract_id, "isDeleted" : False }, {"internalDocumentId": 1, "documentNumber" : 1})
    print(f"SRSA Document: {srsa_doc}")
    preContractSrsa_internalDocumentId = ""
    if srsa_doc : 
        documentNumber = srsa_doc["documentNumber"]
        srsa_pre_contract_doc = db[SRSA_COLLECTION].find_one({"documentNumber": documentNumber, "dueDiligencePhase": "Pre Contract"}, {"internalDocumentId": 1})        
        if srsa_pre_contract_doc:
            preContractSrsa_internalDocumentId = srsa_pre_contract_doc["internalDocumentId"]
    
    if preContractSrsa_internalDocumentId == "":
        validation_log.append({"ReferenceID": reference_id, "Issue": " Pre Contract SRSA document missing"})
        continue

    expected_forms = form_df[form_df["Reference ID*"] == reference_id]
    for _, form in expected_forms.iterrows():
        masterFormId = form.get("Master Form ID*")
        db_form = db[FORM_COLLECTION].find_one({"sourceFormDocumentNumber": masterFormId, "supplierRSAId" : preContractSrsa_internalDocumentId, "isDeleted" : False }, {"internalDocumentId": 1})
        print(f"Validating Form ID: {masterFormId} form Found is {form}")
        if not db_form:
            validation_log.append({
                "ReferenceID": reference_id,
                "FormID": masterFormId,
                "Issue": "Form missing in DB"
            })

    # expected_responses = response_df[response_df["ReferenceID"] == reference_id]
    # for _, resp in expected_responses.iterrows():
    #     response_id = resp.get("ResponseID") or resp.get("FormID")
    #     db_resp = db[RESPONSE_COLLECTION].find_one({
    #         "referenceId": reference_id,
    #         "responseId": response_id
    #     })
    #     if not db_resp:
    #         validation_log.append({
    #             "ReferenceID": reference_id,
    #             "ResponseID": response_id,
    #             "Issue": "Response missing in DB"
    #         })

# --- Output Result ---
if validation_log:
    result_df = pd.DataFrame(validation_log)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    input_file_name = os.path.splitext(os.path.basename(EXCEL_FILE))[0]
    output_file_name = f"{input_file_name}_ValidationResult_{timestamp}.csv"

    # Save the result DataFrame to the constructed file name
    result_df.to_csv(output_file_name, index=False)
    print(f"Validation issues found. See {output_file_name}")
else:
    print("Validation successful. All data matched.")
