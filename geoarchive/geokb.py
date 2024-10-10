import os
from wbmaker import WB

class GeoKB:
    def __init__(self):
        self.check_env()
        self.wb_session()

    def check_env(self):
        REQUIRED_ENV_VARS = [
            'WB_SPARQL_ENDPOINT',
            'WB_URL',
            'MEDIAWIKI_API',
            'WB_BOT_USER_AGENT',
            'WB_BOT_USER',
            'WB_BOT_PASS'
        ]

        for env_var in REQUIRED_ENV_VARS:
            if env_var not in os.environ:
                raise ValueError(f'{env_var} is not set')
            
    def wb_session(self):
        self.wb = WB()

class Ref:
    def __init__(self, geokb_con=None):
        if geokb_con:
            self.geokb_con = geokb_con
        else:
            self.geokb_con = GeoKB()

        self.geokb_commodities()
        self.geokb_places()

    def geokb_commodities(self):
        q = """
        PREFIX wd: <https://geokb.wikibase.cloud/entity/>
        PREFIX wdt: <https://geokb.wikibase.cloud/prop/direct/>

        SELECT ?item ?itemLabel
        WHERE {
            ?item wdt:P1 wd:Q406 .
            SERVICE wikibase:label { bd:serviceParam wikibase:language "en" . }
        }
        """

        df = self.geokb_con.wb.sparql_query(q)
        df['itemLabel'] = df['itemLabel'].str.lower()
        
        self.commodity_lookup = df.set_index('itemLabel')['item'].to_dict()

    def geokb_places(self):
        q = """
        PREFIX wd: <https://geokb.wikibase.cloud/entity/>
        PREFIX wdt: <https://geokb.wikibase.cloud/prop/direct/>

        SELECT ?item ?itemLabel ?geonames_feature_code
        WHERE {
            ?item wdt:P211 ?geonames_feature_code .
            FILTER(STRSTARTS(STR(?geonames_feature_code), "ADM"))
            SERVICE wikibase:label { bd:serviceParam wikibase:language "en" . }
        }
        """

        df = self.geokb_con.wb.sparql_query(q)
        
        self.place_lookup = df.set_index('itemLabel')['item'].to_dict()

class Entity:
    def __init__(self, schema_doc, geokb_con=None):
        self.schema_doc = schema_doc

        if geokb_con:
            self.geokb_con = geokb_con
        else:
            self.geokb_con = GeoKB()

        self.add_or_update_company()
        self.get_item()
        self.entity_from_schema()
        self.write_entity()

    def get_elements(self):
        self.archived_at = next((i['url'] for i in self.schema_doc['identifier'] if i['name'] == 'ScienceBase Item ID'), None)
        if self.archived_at is None:
            raise ValueError('No ScienceBase Item ID found in the schema document')
        
        self.metadata_url = next((i['url'] for i in self.schema_doc['identifier'] if i['name'] == 'Zotero Key'), None)
        if self.metadata_url is None:
            raise ValueError('No Zotero Key/metadata URL found in the schema document')

    def get_item(self):
        q = f"""
        PREFIX wdt: <https://geokb.wikibase.cloud/prop/direct/>

        SELECT ?item
        WHERE {{
            ?item wdt:{self.geokb_con.wb.props['archived at']['property']} ?archived_at .
            FILTER(STR(?archived_at) = "{self.archived_at}")
        }}
        """

        df = self.geokb_con.wb.sparql_query(q)
        if df.empty:
            self.item = self.geokb_con.wb.wbi.item.new()
            self.write_summary = 'created new NI 43-101 entity from schema.org source document'
        else:
            self.item = self.geokb_con.wb.wbi.item.get(df['item'].values[0].split('/')[-1])
            self.write_summary = 'updated NI 43-101 entity from schema.org source document'

    def add_or_update_company(self):
        company_obj = next((i for i in self.schema_doc['about'] if i['additional_type'] == 'company'), None)
        if company_obj is None:
            self.company_item = None
            return

        q = f"""
        PREFIX wdt: <https://geokb.wikibase.cloud/prop/direct/>

        SELECT ?item
        WHERE {{
            ?item wdt:{self.geokb_con.wb.props['SEDAR company identifier']['property']} ?company_id .
            FILTER(STR(?company_id) = "{company_obj['identifier']['value']}")
        }}
        """

        df = self.geokb_con.wb.sparql_query(q)
        if df.empty:
            self.company_item = self.geokb_con.wb.wbi.item.new()
            self.company_item.labels.set('en', company_obj['name'])
            self.company_item.descriptions.set('en', 'commercial company involved in mineral exploration and development that posts securities filings in Canada')

            if 'alternateName' in company_obj:
                self.company_item.aliases.set('en', [company_obj['alternateName']])

            self.company_item.claims.add(
                self.geokb_con.wb.wbi.datatypes.Item(
                    prop_nr=self.geokb_con.wb.props['instance of']['property'],
                    value='Q331123'
                )
            )

            self.company_item.claims.add(
                self.geokb_con.wb.wbi.datatypes.ExternalID(
                    prop_nr=self.geokb_con.wb.props['SEDAR company identifier']['property'],
                    value=company_obj['identifier']['value']
                )
            )

            self.company_item = self.company_item.write(summary='created new company entity referenced in NI 43-101 filing')
        else:
            self.company_item = self.geokb_con.wb.wbi.item.get(df['item'].values[0].split('/')[-1])

    def entity_from_schema(self):
        self.item.labels.set('en', self.schema_doc['name'])
        self.item.descriptions.set('en', self.schema_doc['abstract'])

        self.item.claims.add(
            self.geokb_con.datatypes.Item(
                prop_nr=self.geokb_con.wbi.props['instance of']['property'],
                value='Q10'
            ),
            action_if_exists=self.geokb_con.wbi_enums.ActionIfExists.REPLACE_ALL
        )

        sedar_filing_identifier = next((i['value'] for i in self.schema_doc['identifier'] if i['name'] == 'SEDAR filing identifier'), None)
        self.item.claims.add(
            self.geokb_con.datatypes.ExternalID(
                prop_nr=self.geokb_con.wbi.props['SEDAR filing identifier']['property'],
                value=sedar_filing_identifier
            ),
            action_if_exists=self.geokb_con.wbi_enums.ActionIfExists.REPLACE_ALL
        )

        if self.company_item:
            self.item.claims.add(
                self.geokb_con.datatypes.Item(
                    prop_nr=self.geokb_con.wbi.props['owner']['property'],
                    value=self.company_item.id
                ),
                action_if_exists=self.geokb_con.wbi_enums.ActionIfExists.REPLACE_ALL
            )

        self.item.claims.add(
            self.geokb_con.datatypes.URL(
                prop_nr=self.geokb_con.wbi.props['metadata URL']['property'],
                value=self.schema_doc['url'],
                qualifiers=[
                    self.geokb_con.datatypes.String(
                        prop_nr=self.geokb_con.wbi.props['MIME type']['property'],
                        value='text/html'
                    ),
                    self.geokb_con.datatypes.String(
                        prop_nr=self.geokb_con.wbi.props['MIME type']['property'],
                        value='application/json'
                    )
                ]
            ), action_if_exists=self.geokb_con.wbi_enums.ActionIfExists.REPLACE_ALL
        )

        sb_archive_url = next((i['url'] for i in self.schema_doc['identifier'] if i['name'] == 'ScienceBase Item ID'), None)
        self.item.claims.add(
            self.geokb_con.datatypes.URL(
                prop_nr=self.geokb_con.wbi.props['archivedAt']['property'],
                value=sb_archive_url,
                qualifiers=[
                    self.geokb_con.datatypes.String(
                        prop_nr=self.geokb_con.wbi.props['MIME type']['property'],
                        value='text/html'
                    ),
                    self.geokb_con.datatypes.String(
                        prop_nr=self.geokb_con.wbi.props['MIME type']['property'],
                        value='application/json'
                    )
                ]
            ), action_if_exists=self.geokb_con.wbi_enums.ActionIfExists.REPLACE_ALL
        )

        content_url_claims = []
        for media_obj in self.schema_doc['associatedMedia']:
            content_url_claims.append(
                self.geokb_con.datatypes.URL(
                    prop_nr=self.geokb_con.wbi.props['content URL']['property'],
                    value=media_obj['url'],
                    qualifiers=[
                        self.geokb_con.datatypes.String(
                            prop_nr=self.geokb_con.wbi.props['MIME type']['property'],
                            value=media_obj['encodingFormat']
                        ),
                        self.geokb_con.datatypes.String(
                            prop_nr=self.geokb_con.wbi.props['sha256']['property'],
                            value=media_obj['sha256']
                        ),
                        self.geokb_con.datatypes.Quantity(
                            prop_nr=self.geokb_con.wbi.props['data size']['property'],
                            amount=media_obj['contentSize'],
                        ),
                        self.geokb_con.datatypes.Item(
                            prop_nr=self.geokb_con.wbi.props['access restricted to']['property'],
                            value='Q44210'
                        )
                    ]
                )
            )
        self.item.claims.add(content_url_claims, action_if_exists=self.geokb_con.wbi_enums.ActionIfExists.REPLACE_ALL)

        addresses_place_claims = []
        for loc_obj in self.schema_doc['spatialCoverage']:
            addresses_place_claims.append(
                self.geokb_con.datatypes.Item(
                    prop_nr=self.geokb_con.wbi.props['addresses place']['property'],
                    value=loc_obj['identifier']['value']
                )
            )
        if addresses_place_claims:
            self.item.claims.add(addresses_place_claims, action_if_exists=self.geokb_con.wbi_enums.ActionIfExists.REPLACE_ALL)

        addresses_subject_claims = []
        for subj in self.schema_doc['about']:
            if subj['additional_type'] == 'commodity':
                addresses_place_claims.append(
                    self.geokb_con.datatypes.Item(
                        prop_nr=self.geokb_con.wbi.props['addresses subject']['property'],
                        value=subj['identifier']['value']
                    )
                )
        if addresses_subject_claims:
            self.item.claims.add(addresses_subject_claims, action_if_exists=self.geokb_con.wbi_enums.ActionIfExists.REPLACE_ALL)

    def write_entity(self):
        self.item = self.item.write(summary=self.write_summary)