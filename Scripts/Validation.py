import pandas as pd
from pymongo import MongoClient
from tkinter import Tk
from tkinter.filedialog import askopenfilenames
import os
from datetime import datetime

# --- Configuration ---
MONGO_URI = "mongodb+srv://uatleoeutenantdocteamsro:XnfQ6pixSookjshO@uat-eu-leo.mwfvc.mongodb.net/?ssl=true&authSource=admin&retryWrites=true&readPreference=secondaryPreferred&w=majority&wtimeoutMS=5000&readConcernLevel=majority&retryReads=true&appName=docteamtprmro"
DB_NAME = "uatdomainmodeling"
SRSA_COLLECTION = "riskAssessment_1664901704"
FORM_COLLECTION = "form_1663277990"
RESPONSE_COLLECTION = "responses"

# --- MongoDB Connection ---
def connect_to_db():
    client = MongoClient(MONGO_URI)
    return client[DB_NAME]

# --- File Selection ---
def select_files():
    Tk().withdraw()  # Prevent full GUI window
    print("Please select the migration Excel files...")
    files = askopenfilenames(filetypes=[("Excel Files", "*.xlsx")])
    if not files:
        print("No files selected. Exiting.")
        exit()
    return files

# --- Load Excel Sheets ---
def load_excel_sheets(file_path):
    sheets = pd.read_excel(file_path, sheet_name=None)
    # Skip the first sheet
    sheets = {sheet_name: sheet_data for i, (sheet_name, sheet_data) in enumerate(sheets.items()) if i > 0}
    print(f"Loaded sheets: {sheets.keys()}")
    return sheets

# --- Fetch SRSA Documents ---
def fetch_srsa_documents(db, contract_ids):
    srsa_docs = list(db[SRSA_COLLECTION].find(
        {"revisedContractNumber": {"$in": contract_ids}, "isDeleted": False},
        {"internalDocumentId": 1, "documentNumber": 1, "revisedContractNumber": 1}
    ))
    print(f"Fetched {len(srsa_docs)} Post-Contract SRSA documents")
    return srsa_docs

# --- Fetch Pre-Contract SRSA Documents ---
def fetch_pre_contract_srsa_documents(db, document_numbers):
    pre_contract_srsa_docs = list(db[SRSA_COLLECTION].find(
        {"documentNumber": {"$in": document_numbers}, "dueDiligencePhase": "Pre Contract"},
        {"internalDocumentId": 1, "documentNumber": 1}
    ))
    print(f"Fetched {len(pre_contract_srsa_docs)} Pre-Contract SRSA documents")
    return pre_contract_srsa_docs

# --- Fetch Control Forms ---
def fetch_control_forms(db, pre_contract_srsa_ids):
    control_forms = list(db[FORM_COLLECTION].find(
        {
            "supplierRSAId": {"$in": pre_contract_srsa_ids},
            "isDeleted": False,
            "formType": 6,
        },
        {"sourceFormDocumentNumber": 1, "supplierRSAId": 1, "internalDocumentId": 1}
    ))
    print(f"Fetched {len(control_forms)} Control Forms")
    return control_forms

# --- Validate Data ---
def validate_data(srsa_df, form_df, srsa_doc_map, pre_contract_srsa_doc_map, control_form_map):
    validation_logs = {
        "Supplier Risk Assessment Header": [],
        "Form Details": [],
        "Form Response": [],
    }

    for _, srsa in srsa_df.iterrows():
        reference_id = srsa["Reference ID*"]
        contract_id = srsa["Contract Id"]
        srsa_doc = srsa_doc_map.get(contract_id)
        preContractSrsa_internalDocumentId = ""

        if srsa_doc:
            documentNumber = srsa_doc.get("documentNumber")
            srsa_pre_contract_doc = pre_contract_srsa_doc_map.get(documentNumber)
            if srsa_pre_contract_doc:
                preContractSrsa_internalDocumentId = srsa_pre_contract_doc["internalDocumentId"]

        if preContractSrsa_internalDocumentId == "":
            validation_logs["Supplier Risk Assessment Header"].append({
                "ReferenceID": reference_id,
                "Issue": "Pre Contract SRSA document missing",
                "ContractID": contract_id
            })
            continue

        linked_forms = control_form_map.get(preContractSrsa_internalDocumentId, [])
        found_form_ids = {form["sourceFormDocumentNumber"] for form in linked_forms}
        expected_forms = form_df[form_df["Reference ID*"] == reference_id]

        for _, form in expected_forms.iterrows():
            masterFormId = form.get("Master Form ID*")
            if masterFormId not in found_form_ids:
                validation_logs["Form Details"].append({
                    "ReferenceID": reference_id,
                    "FormID": masterFormId,
                    "Issue": "Form missing in DB",
                    "SRSAID": preContractSrsa_internalDocumentId,
                    "SRSDocumentNumber": documentNumber
                })

    return validation_logs

# --- Save Validation Results ---
def save_validation_results(output_file_name, sheets, validation_logs):
     if any(validation_logs[sheet_name] for sheet_name in validation_logs):
        with pd.ExcelWriter(output_file_name, engine='xlsxwriter') as writer:
            # Write validation logs to corresponding sheets
            for sheet_name, sheet_data in sheets.items():
                # Write original data to the sheet
                if validation_logs[sheet_name]:
                    validation_df = pd.DataFrame(validation_logs[sheet_name])
                    validation_df.to_excel(writer, sheet_name=f"{sheet_name}", index=False)
            print(f"Validation results saved to {output_file_name}")

# --- Main Script ---
def main():
    db = connect_to_db()
    files = select_files()

    for excel_file in files:
        print(f"Processing file: {excel_file}")
        sheets = load_excel_sheets(excel_file)
        srsa_df = sheets["Supplier Risk Assessment Header"]
        form_df = sheets["Form Details"]

        contract_ids = srsa_df["Contract Id"].tolist()
        srsa_docs = fetch_srsa_documents(db, contract_ids)
        srsa_doc_map = {doc["revisedContractNumber"]: doc for doc in srsa_docs}

        document_numbers = [doc["documentNumber"] for doc in srsa_docs if "documentNumber" in doc]
        pre_contract_srsa_docs = fetch_pre_contract_srsa_documents(db, document_numbers)
        pre_contract_srsa_doc_map = {doc["documentNumber"]: doc for doc in pre_contract_srsa_docs}
        pre_contract_srsa_ids = [doc["internalDocumentId"] for doc in pre_contract_srsa_docs]

        control_forms = fetch_control_forms(db, pre_contract_srsa_ids)
        control_form_map = {}
        for form in control_forms:
            supplier_rsa_id = form["supplierRSAId"]
            if supplier_rsa_id not in control_form_map:
                control_form_map[supplier_rsa_id] = []
            control_form_map[supplier_rsa_id].append(form)

        validation_logs = validate_data(srsa_df, form_df, srsa_doc_map, pre_contract_srsa_doc_map, control_form_map)

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        input_file_name = os.path.splitext(os.path.basename(excel_file))[0]
        output_file_name = f"{input_file_name}_ValidationResult_{timestamp}.xlsx"
        save_validation_results(output_file_name, sheets, validation_logs)

if __name__ == "__main__":
    main()