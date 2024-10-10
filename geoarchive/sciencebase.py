import os
from sciencebasepy import SbSession
import re
from datetime import datetime
import pandas as pd
import warnings
import json

from . import calculate_checksum, extract_linkable_terms, extract_date
from . import geokb
from . import zotero

class NI43101Process:
    '''
    This is a specific class with rules and processing for the NI 43-101 Technical Reports.
    Some of the same steps would apply in other cases, but this is an example of how to build a class that handles specific business logic.
    This class handles the communication with the dropbox item in ScienceBase, pulling its current files and handing them off to the NI43101Item class.
    '''
    def __init__(
            self, 
            ni43101_file_archive_item_id = "6618596fd34e7eb9eb7d7b7c",
            ni43101_dropbox_item_id = "66185a07d34e7eb9eb7d7b80",
            zotero_library_id = "4530692",
            cache_path = "/tmp",
            pdf_engine='pdfminer'
        ):

        self.ni43101_file_archive_item_id = ni43101_file_archive_item_id
        self.ni43101_dropbox_item_id = ni43101_dropbox_item_id
        self.zotero_library_id = zotero_library_id
        self.cache_path = cache_path
        self.pdf_engine = pdf_engine

        self.check_env()
        self.sb_session()
        self.get_dropbox()

        try:
            self.geokb = geokb.GeoKB()
            self.geokb_ref = geokb.Ref(geokb_con=self.geokb)
        except Exception as e:
            warnings.warn(f"Failed to initialize GeoKB. Schema will be generated without GeoKB linkages.")
            self.geokb = None
            self.geokb_ref = None

    def check_env(self):
        '''
        Check that the required environment variables are set. We can't run anything here if we can't authenticate to ScienceBase.
        '''
        REQUIRED_ENV_VARS = [
            'SB_ACCESS_TOKEN',
            'SB_REFRESH_TOKEN',
        ]

        for env_var in REQUIRED_ENV_VARS:
            if env_var not in os.environ:
                raise ValueError(f'{env_var} is not set')
            
    def sb_session(self):
        '''
        Initialize the ScienceBase session.
        The sb_token here is tied to an individual user account and is the only supported method of authenticating with ScienceBase
        to access a secure collection.
        '''
        sb_token = {
            "access_token": os.environ['SB_ACCESS_TOKEN'], 
            "refresh_token": os.environ['SB_REFRESH_TOKEN']
        }
        self.sb = SbSession()
        self.sb.add_token(sb_token)

        if not self.sb.is_logged_in():
            raise ValueError("FAILED TO AUTHENTICATE TO ScienceBase")

    def get_dropbox(self):
        '''
        The dropbox item is the item in ScienceBase that contains the PDF files that we want to process.
        Files attached to the dropbox item are expected to be the raw downloads from SEDAR,
        unzipped as individual PDFs with filenames providing certain details about the filing and the company.
        '''
        self.dropbox_item = self.sb.get_item(self.ni43101_dropbox_item_id)

        if 'files' not in self.dropbox_item or len(self.dropbox_item['files']) == 0:
            raise ValueError("No files found in the Dropbox item")
        
    def process_files(self):
        '''
        This function runs the workflow/pipeline to process each file in the dropbox item.
        The resulting archive_item object from invoking the NI43101Item class produces all of the following:
        - A schema.org document with as much information as we can build at this stage
        - A parquet file with the raw text content of the PDF
        - A JSON file with the schema document
        - A new ScienceBase item JSON structure to house the files associated with the filing
        - The returned ScienceBase Item for further actions
        '''
        for f in self.dropbox_item['files']:
            # Build the ScienceBase archive item
            archive_item = NI43101Item(
                sb=self.sb,
                cache_path=self.cache_path,
                sb_file_meta=f, 
                geokb_ref=self.geokb_ref,
                ni43101_file_archive_item_id=self.ni43101_file_archive_item_id
            )

            # Create a Zotero item from the schema document
            zotero_item = zotero.Zot(
                library_id=self.zotero_library_id,
                schema_doc=archive_item.schema_doc
            )

            # Update the schema document with the Zotero item URL
            archive_item.schema_doc['url'] = f"https://w3id.org/usgs/z/{self.zotero_library_id}/{zotero_item.item['key']}"

            # Update the schema.org document in the ScienceBase item
            schema_json_path = os.path.join(self.cache_path, f"{archive_item.sedar_filing_id}.json")
            json.dump(archive_item.schema_doc, open(schema_json_path, 'w'))
            self.sb.replace_file(
                filename=schema_json_path,
                item=archive_item.sb_item
            )

            # Update the ScienceBase Item with the Zotero item URL
            archive_item.sb_item['webLinks'] = [
                {
                    "type": "metadata URL",
                    "typeLabel": "metadata URL",
                    "uri": zotero_item.item['url'],
                    "title": "Zotero metadata landing page",
                    "hidden": False
                }
            ]
            archive_item.sb_item = self.sb.update_item(archive_item.sb_item)

class NI43101Item:
    def __init__(self, sb, cache_path, sb_file_meta, geokb_ref, ni43101_file_archive_item_id):
        self.sb = sb
        self.cache_path = cache_path
        self.sb_file_meta = sb_file_meta
        self.geokb_ref = geokb_ref
        self.ni43101_file_archive_item_id = ni43101_file_archive_item_id
        
        # workflow steps
        self.build_schema_doc()
        self.parse_pdf_text()
        self.extract_effective_date()

        if self.geokb_ref:
            self.extract_locations()
            self.extract_commodities()

        self.rename_report()

        self.prep_files()

        self.sb_archive_item()
        self.upsert_sb()

    def build_schema_doc(self):
        '''
        This initial workflow step builds the schema.org document for the file using information pulled from the file name.
        It also downloads the PDF file to the provided cache path for further processing.
        The function yields an initial version of the schema.org document with as much information as we can build at this stage.

        Args:
            sb_file_meta (dict): The raw metadata for the file from ScienceBase
        '''
        self.schema_doc = {
            "@context": "https://schema.org",
            "@type": "CreativeWork",
            "additionalType": "NI 43-101 Technical Report",
            "name": self.sb_file_meta['name'],
            "abstract": "an NI 43-101 Technical Report sourced from the GeoArchive collection",
            "identifier": [],
            "associatedMedia": [],
            "about": [],
            "spatialCoverage": []
        }

        # Get stuff from the long file name
        name_parts = [i.strip() for i in self.sb_file_meta['name'].split('/')]

        # The filing ID is ultimately used to name the files associated with that filing in a consistent manner
        filing_id = name_parts[2].split(' ')[0]
        self.schema_doc['identifier'].append({
            "@type": "PropertyValue",
            "name": "SEDAR filing identifier",
            "value": filing_id
        })

        filing_type = name_parts[2].split(' ')[-1]
        self.schema_doc['additionalType'] = f"NI 43-101 Filing ({filing_type})"

        # Mining company information is used to create organization entities in the knowledge graph representation
        mining_company = {
            "@type": "Organization",
            "additionalType": "company",
            "name": name_parts[1].split('(')[0].strip(),
            "identifier": {
                "@type": "PropertyValue",
                "propertyID": "SEDAR Company ID",
                "value": name_parts[0].split(' ')[0]
            }
        }

        # Filenames often contain former names of companies, helping to round out identifying information
        mining_company_qualified_name = name_parts[0].replace(mining_company['identifier']['value'], '').strip()
        match = re.search(r'\((.*?)\)', mining_company_qualified_name)

        if match:
            company_parenthetical = match.group(1)
            if company_parenthetical.lower().startswith('formerly'):
                mining_company['alternateName'] = company_parenthetical.split('formerly')[-1].replace('"', '').strip()

        self.schema_doc['about'].append(mining_company)

        # Encode initial information on the file
        sb_file_id = self.sb_file_meta['url'].split('%2F')[-1]
        local_file_name = f"{sb_file_id}.pdf"
        local_file_path = os.path.join(self.cache_path, local_file_name)

        if not os.path.exists(local_file_path):
            self.sb.download_file(
                url=self.sb_file_meta['url'],
                local_filename=local_file_name,
                destination=self.cache_path
            )

        # The original ScienceBase File ID from the dropbox is essentially meaningless after we process, rename the file, and load it to a new item
        # where it will have a completely new file identifier. Retaining it in the schema document provides a level of provenance tracing that could
        # be useful in some circumstances.
        pdf_media = {
            "@type": "MediaObject",
            "additionalType": "main content",
            "name": f"PDF Content ({sb_file_id})",
            "alternateName": self.sb_file_meta['name'],
            "contentSize": os.path.getsize(local_file_path),
            "encodingFormat": "application/pdf",
            "sha256": calculate_checksum(local_file_path, 'sha256'),
            "md5": calculate_checksum(local_file_path, 'md5'),
            "identifier": {
                "@type": "PropertyValue",
                "name": "Original ScienceBase File ID",
                "value": sb_file_id
            }
        }
        self.schema_doc['associatedMedia'].append(pdf_media)

    def parse_pdf_text(self):
        '''
        This workflow step extracts the text content from the PDF file, saving a data structure in parquet format with the raw text, page numbers,
        and checksum for the file to be used in further processing steps, both at this stage and in later data extraction work.
        The function yields the schema document with the extracted text content media object added to the associatedMedia list.
        '''
        source_media_object = next((i for i in self.schema_doc['associatedMedia'] if i['additionalType'] == 'main content'), None)
        if not source_media_object:
            raise ValueError('No main content media object found in schema_doc')
        
        source_file_path = os.path.join(self.cache_path, f"{source_media_object['identifier']['value']}.pdf")
        parquet_file_path = os.path.join(self.cache_path, f"{source_media_object['identifier']['value']}.parquet")

        if os.path.exists(parquet_file_path):
            self.pages = pd.read_parquet(parquet_file_path)
        else:
            pages = []
            if self.pdf_engine == 'pdfminer':
                from pdfminer.high_level import extract_pages
                from pdfminer.layout import LTTextContainer

                page_count = 0
                for page_layout in extract_pages(source_file_path):
                    page_count+=1
                    p = {
                        'page_num': page_count,
                        'timestamp': datetime.now().isoformat(),
                        'sha256': source_media_object['sha256'],
                        'page_content': None
                    }
                    text_chunks = []
                    for element in page_layout:
                        if isinstance(element, LTTextContainer) and len(element.get_text().strip()) > 0:
                            text_chunks.append(element.get_text())
                    if text_chunks:
                        p['page_content'] = '\n'.join(text_chunks)
                    pages.append(p)
            elif self.pdf_engine == 'pypdf':
                import PyPDF2

                pdf_reader = PyPDF2.PdfReader(source_file_path)
                for page_num, page in enumerate(pdf_reader.pages):
                    page_content = page.extract_text()
                    if isinstance(page_content, str):
                        p = {
                            'page_num': page_num,
                            'timestamp': datetime.now().isoformat(),
                            'sha256': source_media_object['sha256'],
                            'page_content': page.extract_text()
                        }
                        pages.append(p)

            self.pages = pd.DataFrame(pages)
            self.pages.to_parquet(parquet_file_path)

        parquet_media_object = next((i for i in self.schema_doc['associatedMedia'] if i['additionalType'] == 'extracted text content'), None)

        if not parquet_media_object:
            parquet_media = {
                "@type": "MediaObject",
                "additionalType": "extracted text content",
                "name": f"Page Text Content ({source_media_object['identifier']['value']})",
                "contentSize": os.path.getsize(parquet_file_path),
                "encodingFormat": "application/vnd.apache.parquet",
                "sha256": calculate_checksum(parquet_file_path, 'sha256'),
                "md5": calculate_checksum(parquet_file_path, 'md5'),
                "identifier": {
                    "@type": "PropertyValue",
                    "name": "ScienceBase File Source ID",
                    "value": source_media_object['identifier']['value']
                }
            }

            self.schema_doc['associatedMedia'].append(parquet_media)

            self.schema_doc['numberOfPages'] = len(self.pages)
        
    def extract_effective_date(self):
        '''
        We make a big assumption here that the first page of the document will contain a date that is the effective date of the filing
        (an important identifying feature). This is not always the case, but it is a common enough practice that it is a good place to start.
        The function called here finds dates that can be validated on the first page of text and selects for the most recent date to use. 
        The date is formatted as ISO text and added to the datePublished field in the schema document.
        '''
        first_page = self.pages.iloc[0]['page_content']
        date_in_first_page = extract_date(first_page)
        if date_in_first_page:
            self.schema_doc['datePublished'] = date_in_first_page
        
    def extract_locations(self):
        '''
        This function parses through each page in the document to find named places based solely on the slate of administrative place names
        organized into the Geoscience Knowledgebase. It then selects for the most predominant place names found with Z-score normalization, selecting
        for cases with a Z-score above 2. The function then adds these place names to the spatialCoverage field in the schema document, including their
        associated GeoKB identifiers for use in linking the document to the knowledge graph. Place names are used in the Zotero representation as tags.
        '''
        locations_in_texts = extract_linkable_terms(
            documents=self.pages['page_content'].to_list(),
            terms=self.geokb_ref.place_lookup
        )
        if locations_in_texts:
            for loc_name, loc_id in locations_in_texts.items():
                loc_obj = {
                    "@type": "Place",
                    "name": loc_name,
                    "identifier": {
                        "@type": "PropertyValue",
                        "name": "GeoKB ID",
                        "value": loc_id
                    }
                }
                self.schema_doc['spatialCoverage'].append(loc_obj)
        
    def extract_commodities(self):
        '''
        This function parses through each page in the document to find commodity names based solely on the slate of commodity names and identifiers
        organized into the Geoscience Knowledgebase. It then selects for the most predominant comm,odities found with Z-score normalization, selecting
        for cases with a Z-score above 2. The function then adds these commodity names and their GeoKB identifiers to the about information in the
        schema document. Commodity entities are linked in the GeoKB as addresses subject claims and names are used in the Zotero representation as tags.
        '''
        commodities_in_texts = extract_linkable_terms(
            documents=[d.lower() for d in self.pages['page_content']],
            terms=self.geokb_ref.commodity_lookup
        )
        if commodities_in_texts:
            for commodity_name, commodity_id in commodities_in_texts.items():
                about_obj = {
                    "@type": "Thing",
                    "additionalType": "commodity",
                    "name": commodity_name,
                    "identifier": {
                        "@type": "PropertyValue",
                        "name": "GeoKB ID",
                        "value": commodity_id
                    }
                }
                self.schema_doc['about'].append(about_obj)
        
    def rename_report(self):
        '''
        This function renames the report to include the filing type and the effective date in the name field.
        This is not as descriptive as extracting the full title, but it saves needing to run a complicated and costly LLM extraction process 
        at this stage.
        '''
        self.schema_doc['name'] = f"{self.schema_doc['additionalType']} filed for {self.schema_doc['about'][0]['name']} (effective date {self.schema_doc['datePublished']})"
    
    def prep_files(self):
        '''
        This function prepares the files for loading into a new ScienceBase item. It renames the files to match the SEDAR filing ID and moves them
        to the cache path for the new item. It also writes the schema document to a JSON file in the cache path. The function returns the list of files
        that are used to generate the new ScienceBase archive item container that will house files associated with the filing.
        '''
        self.sedar_filing_id = next((i['value'] for i in self.schema_doc['identifier'] if i['name'] == 'SEDAR Filing ID'), None)
        sb_file_id = next((i['identifier']['value'] for i in self.schema_doc['associatedMedia'] if i['additionalType'] == 'main content'), None)
        self.item_files = []

        pdf_file_path = os.path.join(self.cache_path, f"{self.sedar_filing_id}.pdf")
        os.rename(
            os.path.join(self.cache_path, f"{sb_file_id}.pdf"), 
            pdf_file_path
        )
        self.item_files.append(pdf_file_path)

        parquet_file_path = os.path.join(self.cache_path, f"{self.sedar_filing_id}.parquet")
        os.rename(
            os.path.join(self.cache_path, f"{sb_file_id}.parquet"), 
            parquet_file_path
        )
        self.item_files.append(parquet_file_path)

        json_file_path = os.path.join(self.cache_path, f"{self.sedar_filing_id}.json")
        json.dump(self.schema_doc, open(json_file_path, 'w'))
        self.item_files.append(json_file_path)

    def sb_archive_item(self):
        '''
        This function creates a new ScienceBase item JSON structure to house the files associated with the filing.
        It uses the schema document to populate the metadata for the new item, including the name, description, and other fields. 
        The function returns the SB item json structure that can be used to create the item as files are uploaded.
        '''
        self.sb_item_shell = {
            "parentId": self.ni43101_file_archive_item_id,
            "title": f"file archive for SEDAR+ filing ID: {self.sedar_filing_id}",
            "identifiers": [
                {
                    "type": "SEDAR Filing ID",
                    "scheme": "https://www.sciencebase.gov/vocab/identifier/term/sedar-filing-id",
                    "key": self.sedar_filing_id
                }
            ]
        }

    def upsert_sb(self):
        '''
        This function uploads the files and the item to ScienceBase and then removed the local cached files.
        '''
        self.sb_item = self.sb.upload_files_and_upsert_item(
            item=self.sb_item_shell, 
            filenames=self.item_files, 
            scrape_file=False
        )

        # We add the w3id.org form of the ScienceBase item ID to the schema document for linking purposes
        # This becomes the archiveLocation attribute in Zotero and the archivedAt claim in the GeoKB
        self.schema_doc['identifier'].append({
            "@type": "PropertyValue",
            "name": "ScienceBase Item ID",
            "value": self.sb_item['id'],
            "url": f"https://w3id.org/usgs/sb/{self.sb_item['id']}"
        })

        # Using the MD5 checksum recorded previously, we can verify the onboard ScienceBase file object and add its URL to the schema document
        for media in self.schema_doc['associatedMedia']:
            sb_file_obj = next((f for f in self.sb_item['files'] if f['checksum']['value'] == media['md5']), None)
            if not sb_file_obj:
                raise ValueError(f"Failed to find ScienceBase file object for {media['name']}")
            media['url'] = sb_file_obj['url']
            del media['md5']

        # Changes to the schema document are saved and reloaded into the ScienceBase item        
        schema_json_path = os.path.join(self.cache_path, f"{self.sedar_filing_id}.json")
        json.dump(self.schema_doc, open(schema_json_path, 'w'))
        self.sb_item = self.sb.replace_file(
            filename=schema_json_path,
            item=self.sb_item
        )

        # Remove the local processing files
        for f in self.item_files:
            os.remove(f)
