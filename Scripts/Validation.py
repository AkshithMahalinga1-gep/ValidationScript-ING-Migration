import pandas as pd
from pymongo import MongoClient
from tkinter import Tk
from tkinter.filedialog import askopenfilenames
import os
from datetime import datetime
import time
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import ProcessPoolExecutor
from concurrent.futures import ThreadPoolExecutor
from mongoCalls import apiCall, fetch_forms_responses
from validationHelper import validate_response_data



# --- Configuration ---
MONGO_URI = "mongodb+srv://uatleoeutenantdocteamsro:XnfQ6pixSookjshO@uat-eu-leo.mwfvc.mongodb.net/?ssl=true&authSource=admin&retryWrites=true&readPreference=secondaryPreferred&w=majority&wtimeoutMS=5000&readConcernLevel=majority&retryReads=true&appName=docteamtprmro"
DB_NAME = "uatdomainmodeling"
SRSA_COLLECTION = "riskAssessment_1664901704"
FORM_COLLECTION = "form_1663277990"
RESPONSE_COLLECTION = "documentQuestionnaire_1695405087"

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
    return sheets

# --- Fetch SRSA Documents ---
def fetch_srsa_documents(db, contract_ids):
    print(f"Fetching Post Contract SRSA documents for contract IDs...")
    srsa_docs = list(db[SRSA_COLLECTION].find(
        {"revisedContractNumber": {"$in": contract_ids}, "isDeleted": False},
        {"internalDocumentId": 1, "documentNumber": 1, "revisedContractNumber": 1}
    ))
    print(f"Fetched {len(srsa_docs)} Post-Contract SRSA documents")
    return srsa_docs

# --- Fetch Pre-Contract SRSA Documents ---
def fetch_pre_contract_srsa_documents(db, document_numbers):
    print(f"Fetching Pre Contract SRSA documents for document numbers...")
    pre_contract_srsa_docs = list(db[SRSA_COLLECTION].find(
        {"documentNumber": {"$in": document_numbers}, "dueDiligencePhase": "Pre Contract"},
        {"internalDocumentId": 1, "documentNumber": 1}
    ))
    print(f"Fetched {len(pre_contract_srsa_docs)} Pre-Contract SRSA documents")
    return pre_contract_srsa_docs

# --- Fetch Control Forms ---
def fetch_control_forms(db, pre_contract_srsa_ids):
    print(f"Fetching Control Forms for Pre Contract SRSAs...")
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

from concurrent.futures import ThreadPoolExecutor

def validate_data(srsa_df, form_df, srsa_doc_map, pre_contract_srsa_doc_map, control_form_map, response_df_excel, form_responses_mongo):
    validation_logs = {
        "Supplier Risk Assessment Header": [],
        "Form Details": [],
        "Form Response": [],
    }

    def validate_srsa_row(srsa):
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
            return {
                "log_type": "Supplier Risk Assessment Header",
                "log": {
                    "ReferenceID": reference_id,
                    "Issue": "Pre Contract SRSA document missing",
                    "ContractID": contract_id
                }
            }

        linked_forms = control_form_map.get(preContractSrsa_internalDocumentId, [])
        found_form_ids = {form["sourceFormDocumentNumber"] for form in linked_forms}
        expected_forms = form_df[form_df["Reference ID*"] == reference_id]

        form_logs = []
        for _, form in expected_forms.iterrows():
            masterFormId = form.get("Master Form ID*")
            if masterFormId not in found_form_ids:
                form_logs.append({
                    "ReferenceID": reference_id,
                    "FormID": masterFormId,
                    "Issue": "Form missing in DB",
                    "SRSAID": preContractSrsa_internalDocumentId,
                    "SRSDocumentNumber": documentNumber
                })
            else:
                matching_form_mongo = next((linked_form for linked_form in linked_forms if form["Master Form ID*"] == linked_form["sourceFormDocumentNumber"]), None)
                if matching_form_mongo:
                    validate_response_data(validation_logs, form, matching_form_mongo, response_df_excel, form_responses_mongo)

        return {
            "log_type": "Form Details",
            "log": form_logs
        }

    # Parallelize validation of rows
    with ThreadPoolExecutor() as executor:
        results = executor.map(validate_srsa_row, srsa_df.to_dict(orient="records"))

    # Aggregate results
    for result in results:
        if result["log_type"] == "Supplier Risk Assessment Header":
            validation_logs["Supplier Risk Assessment Header"].append(result["log"])
        elif result["log_type"] == "Form Details":
            validation_logs["Form Details"].extend(result["log"])

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
    else:
        print(f"No validation issues found for {output_file_name}. No file created.")

def load_file_data(file_path):
    """Helper function to load data from a single file."""
    sheets = load_excel_sheets(file_path)
    srsa_df = sheets["Supplier Risk Assessment Header"]
    return srsa_df["Contract Id"].tolist()

def load_questionNumber(file_path):
    """Helper function to load data from a single file."""
    sheets = load_excel_sheets(file_path)
    formResponse_df = sheets["Form Response"]
    return formResponse_df["Question Number [QB Number]*"].drop_duplicates().tolist()


def fetch_all_data(db, files):
    all_contract_ids = set()

    # Load files in parallel
    with ThreadPoolExecutor(max_workers=10) as executor:
        results = executor.map(load_file_data, files)

    for contract_ids in results:
        all_contract_ids.update(contract_ids)

    all_contract_ids = list(all_contract_ids)

    # Fetch SRSA documents
    srsa_docs = fetch_srsa_documents(db, all_contract_ids)
    srsa_doc_map = {doc["revisedContractNumber"]: doc for doc in srsa_docs}

    all_document_numbers = [doc["documentNumber"] for doc in srsa_docs if "documentNumber" in doc]

    pre_contract_srsa_docs = fetch_pre_contract_srsa_documents(db, all_document_numbers)
    pre_contract_srsa_doc_map = {doc["documentNumber"]: doc for doc in pre_contract_srsa_docs}
    pre_contract_srsa_ids = [doc["internalDocumentId"] for doc in pre_contract_srsa_docs]

    control_forms = fetch_control_forms(db, pre_contract_srsa_ids)

    form_responses = fetch_forms_responses(db, control_forms, RESPONSE_COLLECTION)
    
    with ThreadPoolExecutor(max_workers=10) as executor:
        questionNumbers = executor.map(load_questionNumber, files)
        
    distinct_questionNumbers = set()
    for qn_list in questionNumbers:
        distinct_questionNumbers.update(qn_list)
    
    questionMappings = apiCall(list(distinct_questionNumbers))
    
    control_form_map = {}
    for form in control_forms:
        supplier_rsa_id = form["supplierRSAId"]
        if supplier_rsa_id not in control_form_map:
            control_form_map[supplier_rsa_id] = []
        control_form_map[supplier_rsa_id].append(form)

    # Update form_responses with questionMappings values
    for form_response in form_responses:
        for questionnaire_detail in form_response.get("questionnaireDetails", []):
            for question in questionnaire_detail.get("questions", []):
                question_library_id = question.get("questionLibraryQuestionId")
                if questionMappings and question_library_id in questionMappings:
                    # Add the value from questionMappings to the question object
                    question["mappedQuestionId"] = questionMappings[question_library_id]

    return srsa_doc_map, pre_contract_srsa_doc_map, control_form_map, form_responses

def process_file(excel_file, srsa_doc_map, pre_contract_srsa_doc_map, control_form_map, form_responses):
    """Process a single Excel file."""
    print(f"Processing file: {excel_file}")
    sheets = load_excel_sheets(excel_file)
    srsa_df = sheets["Supplier Risk Assessment Header"]
    form_df = sheets["Form Details"]
    response_df = sheets["Form Response"]

    # Validate data for the current file
    validation_logs = validate_data(srsa_df, form_df, srsa_doc_map, pre_contract_srsa_doc_map, control_form_map, response_df, form_responses)
    
    output_folder = "Validation Result"
    os.makedirs(output_folder, exist_ok=True)

    # Save validation results
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    input_file_name = os.path.splitext(os.path.basename(excel_file))[0]
    output_file_name = os.path.join(output_folder, f"{input_file_name}_ValidationResult_{timestamp}.xlsx")
    save_validation_results(output_file_name, sheets, validation_logs)

    print(f"Finished processing file: {excel_file}")

# --- Main Script ---
def main():
    start_time = time.time()  # Record the start time
    db = connect_to_db()
    files = select_files()

    print("Fetching all data...")
    srsa_doc_map, pre_contract_srsa_doc_map, control_form_map, form_responses = fetch_all_data(db, files)
    print(f"Data fetching completed in {time.time() - start_time:.2f} seconds.")

    batch_size = 5  # Number of files to process in each batch
    file_batches = [files[i:i + batch_size] for i in range(0, len(files), batch_size)]

    print("Processing files...")
    with ThreadPoolExecutor(max_workers=10) as executor:
            for batch in file_batches:
                executor.map(process_file, batch, [srsa_doc_map] * len(batch), [pre_contract_srsa_doc_map] * len(batch), [control_form_map] * len(batch), [form_responses] * len(batch))
    print(f"File processing completed in {time.time() - start_time:.2f} seconds.")

    end_time = time.time()  # Record the end time
    print(f"Total execution time: {end_time - start_time:.2f} seconds")

if __name__ == "__main__":
    main()
   