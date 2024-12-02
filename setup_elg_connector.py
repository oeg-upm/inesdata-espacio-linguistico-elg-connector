import requests
import json
import os
import logging
from time import sleep
from elg import Catalog, Entity
from elg.utils.utils import ISO639
from requests.exceptions import ConnectionError


logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

RESOURCE_TYPES = ["Corpus", "Lexical/Conceptual resource"]
TYPE_MAPPING={"Corpus":"corpus","LexicalConceptualResource":"lexical_resource"}
OPEN_LICENSES = ["Apache-2.0","CC-BY-4.0", "CC-BY-NC-4.0", "CC-BY-NC-ND-4.0", "CC-BY-NC-SA-2.0", "CC-BY-NC-SA-2.5","CC-BY-NC-SA-4.0","CC-BY-SA-4.0", "CC0-1.0"]
AUTHENTICATION_ENDPOINT = os.environ.getenv("AUTHENTICATION_ENDPOINT", "http://keycloak:8080/realms/dataspace/protocol/openid-connect/token")
ELG_CONNECTOR_ENDPOINT = os.environ.getenv("ELG_CONNECTOR_ENDPOINT", "http://connector-elg:39193")
CONNECTOR_ASSETS_ENDPOINT = ELG_CONNECTOR_ENDPOINT + "/management/v3/assets"
CONNECTOR_PAGINATION_ENDPOINT = ELG_CONNECTOR_ENDPOINT + "/management/pagination"
CONNECTOR_CONTRACTDEFINITION_ENDPOINT = ELG_CONNECTOR_ENDPOINT + "/management/v3/contractdefinitions"
USER = os.environ.getenv("USER")
PASSWORD = os.environ.getenv("PASSWORD")
ELG_WS_ENDPOINT = os.environ.getenv("ELG_WS_ENDPOINT", "http://elg-connector-ws:5000/") 

catalog = Catalog()

def authenticate(user,password):
    payload='client_id=dataspace-users&grant_type=password&username=%s&password=%s' % (user,password)
    headers = {
  'Content-Type': 'application/x-www-form-urlencoded'
}
    response = requests.request("POST", AUTHENTICATION_ENDPOINT, headers=headers, data=payload)
    return response.json()["access_token"]

def check_asset(id):
    jwt = authenticate(USER,PASSWORD)
    headers = {
  'Authorization': f'Bearer {jwt}'
}
    url = os.path.join(CONNECTOR_ASSETS_ENDPOINT, str(id))
    response = requests.request("GET", url, headers=headers)
    if response.status_code == 200:
        return True
    elif response.status_code == 404:
        return False
    else:
        raise Exception(f"There was an error calling the url: {url} \nresponse: {response.text}")

def language_to_iso(language):
  codes = ISO639.LanguageCodes(language)
  if type(codes)==list:
    iso_code = codes[-1]
  else:
    iso_code = language
  return iso_code 

def create_asset(id, name="", asset_type="corpus", description="", languages=[], resource_type=None, creation_date=None):
    jwt = authenticate(USER,PASSWORD)
    if type(languages)==str and (language_to_iso(languages)=="ast" or len(language_to_iso(languages))==2):
      languages=[{"id":"0", "name": language_to_iso(languages)}]
    else:
      languages=[{"id":"0", "name": language_to_iso(language)} for language in languages if (language_to_iso(language)=="ast" or len(language_to_iso(language))==2)]
    payload = json.dumps({
  "@context": {
    "@vocab": "https://w3id.org/edc/v0.0.1/ns/",
    "dcterms":"http://purl.org/dc/terms/"

  },
  "@id": str(id),
  "properties": {
    "name": name,
    "contenttype": "application/zip",
    "assetType": TYPE_MAPPING[resource_type],
    "dcterms:description": description,
    "assetData":{
      "languages": languages
    },
    "created_at": creation_date
  },
  "dataAddress": {
    "type": "HttpData",
    "name": name,
    "baseUrl": f"{ELG_WS_ENDPOINT}{str(id)}",
    "proxyPath": "true"
  }
})
    headers = {
  'Authorization': f'Bearer {jwt}',
  'Content-Type': 'application/json'
}
    response = requests.request("POST", CONNECTOR_ASSETS_ENDPOINT, headers=headers, data=payload)
    if response.status_code!=200:
        raise Exception(f"There was an error calling connector endpoint: {CONNECTOR_ASSETS_ENDPOINT} \nresponse: {response.text}")

def get_assets():
    jwt = authenticate(USER,PASSWORD)
    payload = json.dumps({
  "@context": {
    "@vocab": "https://w3id.org/edc/v0.0.1/ns/"
  },
  "offset": 0,
  "limit": 5,
  "sortOrder": "ASC",
  "sortField": "id",
  "filterExpression": []
})
    headers = {
  'Authorization': f'Bearer {jwt}',
  'Content-Type': 'application/json'
}
    try:
        response = requests.request("POST", CONNECTOR_ASSETS_ENDPOINT+"/request", headers=headers, data=payload)
        return response.status_code
    except ConnectionError as error:
        logger.exception(error)
        return -1

def count_contracts():
    jwt = authenticate(USER,PASSWORD)
    payload = json.dumps({
  "@context": {
    "@vocab": "https://w3id.org/edc/v0.0.1/ns/"
  },
  "filterExpression": []
})
    headers = {
  'Authorization': f'Bearer {jwt}',
  'Content-Type': 'application/json'
}
    response = requests.request("POST", CONNECTOR_PAGINATION_ENDPOINT+"/count?type=contractDefinition", headers=headers, data=payload)
    if response.status_code!=200:
        raise Exception(f"There was an error calling connector endpoint: {CONNECTOR_PAGINATION_ENDPOINT} \nresponse: {response.text}")
    return response.text

def create_contract():
    jwt = authenticate(USER,PASSWORD)
    payload = json.dumps({
  "@context": {
    "@vocab": "https://w3id.org/edc/v0.0.1/ns/"
  },
  "@id": "always-true",
  "accessPolicyId": "always-true",
  "contractPolicyId": "always-true",
  "assetsSelector": []
})
    headers = {
  'Authorization': f'Bearer {jwt}',
  'Content-Type': 'application/json'
}
    response = requests.request("POST", CONNECTOR_CONTRACTDEFINITION_ENDPOINT, headers=headers, data=payload)
    if response.status_code!=200:
        raise Exception(f"There was an error calling connector endpoint: {CONNECTOR_CONTRACTDEFINITION_ENDPOINT} \nresponse: {response.text}")

# Check if elg connector is available
while get_assets() != 200:
    logger.info("ELG connector is not ready yet. Waiting 10 seconds to retry...")
    sleep(10)

for resource in RESOURCE_TYPES:
  for license in OPEN_LICENSES:
      results = catalog.search(
          resource = resource,
          license = license,
          elg_hosted_data = True,
          limit = 100,
      )

      for result in results:
          asset_exists = check_asset(result.id)
          if not asset_exists:
              logger.info(f"{str(result.id)} is not in the catalogue, inserting it...")
              entity = Entity.from_id(result.id)
              create_asset(entity.id, name=entity.resource_name, description=entity.description, languages=entity.languages, resource_type=entity.resource_type, creation_date=entity.creation_date)
              logger.info(f"{str(result.id)} has been inserted.")
          else:
              logger.info(f"{str(result.id)} is already in the catalogue.")

# Check if there is a contract
if count_contracts() == "0":
    create_contract()
else:
    logger.info("A contract already exists, skipping creation...")


