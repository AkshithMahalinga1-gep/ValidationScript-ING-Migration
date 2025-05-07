import requests
from API_Details import neo4j_api
from pymongo import MongoClient

# --- Fetch Control Forms ---
def fetch_forms_responses(db, forms, RESPONSE_COLLECTION):
    form_ids = [doc["internalDocumentId"] for doc in forms]
    control_forms_responses = list(db[RESPONSE_COLLECTION].find(
        {
            "internalDocumentId": {"$in": form_ids},
            "isLive": True,
        },
        {"internalDocumentId": 1, "documentNumber": 1, "questionnaireDetails.questions.questionId": 1, "questionnaireDetails.questions.questionLibraryQuestionId": 1, "questionnaireDetails.questions.responseValue": 1, "questionnaireDetails.questions.responseAttachment": 1},
    ))
    print(f"Fetched {len(control_forms_responses)} Control Forms Responses")
    return control_forms_responses

def apiCall(distinct_questionNumbers):
    url = neo4j_api["url"]
    headers = {
        "accept": "application/json",
        "Content-Type": "application/json-patch+json",
        "Authorization": neo4j_api["Authorization"],
        }
    payload = {
        "ClientId": "70022563",
        "AppId": "1090",
        "PluginId": "1",
        "PluginVersion": "1",
        "OperationName": "GetMasterFormQuestionsAndResponses",
        "Version": "1",
        "Variables": {
            "documentNumber": distinct_questionNumbers
        },
        "TransactionId": "955806cc-e968-44af-a766-ca58c79ab538",
        "IsRetry": True,
        "DacThumbprint": None,
        "QueryResolverSettings": {
            "BaseUrl": None,
            "AcsAppClientId": None,
            "JWToken": None,
            "TransactionScopeId": None
        }
    }

    response = requests.post(url, headers=headers, json=payload)
    print(f"API Response Status Code: {response.status_code}")
    if response.status_code != 200:
        print(f"Error: {response.text}")
        return None
    response_data = response.json()
    question_mapping = {item['questionId']: item['questionNumber'] for item in response_data.get('ouputData', [])}
    return question_mapping