import os
from pyzotero import zotero
from dateutil import parser

class Zot:
    def __init__(self, library_id: str, schema_doc: dict, commit: bool = False):
        self.library_id = library_id
        self.schema_doc = schema_doc

        self.check_env()
        self.z_session()
        self.z_item_from_schema()
        if commit:
            self.commit()

    def check_env(self):
        '''
        Check that the required environment variables are set. We can't run anything here if we can't authenticate to Zotero.
        '''
        REQUIRED_ENV_VARS = [
            'ZOTERO_API_KEY'
        ]

        for env_var in REQUIRED_ENV_VARS:
            if env_var not in os.environ:
                raise ValueError(f'{env_var} is not set')
            
    def z_session(self):
        '''
        Initialize the Zotero API connection.
        '''
        self.z = zotero.Zotero(
            self.library_id,
            'group',
            os.environ['ZOTERO_API_KEY']
        )

    def z_item_from_schema(self):
        '''
        Create a Zotero item from a schema.org CreativeWork document.
        This currently handles the case of a "report" type item only with a focus on the NI 43-101 use case.
        It needs more refinement to handle additional use cases.
        The final step here is a roundtrip back to Zotero to update the URL with the w3id.org link.
        '''
        if not self.schema_doc:
            self.z_item = None
            return
        
        self.z_item = {
            'itemType': 'report'
        }
        self.z_item['title'] = self.schema_doc['name']
        self.z_item['reportType'] = self.schema_doc['additionalType']
        self.z_item['date'] = str(parser.parse(self.schema_doc['datePublished']).year)
        self.z_item['pages'] = self.schema_doc['numberOfPages']
        self.z_item['language'] = 'en'
        self.z_item['archive'] = 'ScienceBase'
        self.z_item['archiveLocation'] = next((i['url'] for i in self.schema_doc['identifier'] if i['name'] == 'ScienceBase Item ID'), '')
        self.z_item['tags'] = []

        for loc_obj in self.schema_doc['spatialCoverage']:
            self.z_item['tags'].append({'tag': f"location:{loc_obj['name']}"})

        for about_obj in self.schema_doc['about']:
            self.z_item['tags'].append({'tag': f"{about_obj['additionalType']}:{about_obj['name']}"})

    def commit(self):
        '''
        Commit the Zotero item to the Zotero library and update the URL with the w3id.org link.
        '''
        z_create_response = self.z.create_items([self.z_item])

        self.item = z_create_response['successful']['0']['data']

        self.item['url'] = f"https://w3id.org/usgs/z/{self.library_id}/{self.z_item['key']}"
        self.z.update_item(self.z_item)

        
