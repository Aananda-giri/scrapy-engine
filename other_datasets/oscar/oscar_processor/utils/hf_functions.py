from huggingface_hub import HfFileSystem, hf_hub_download

import logging
import zstandard as zstd
import io
import json
import os
from pydantic import BaseModel, Field, ValidationError
from typing import List, Dict, Optional

# Configure logging
logger = logging.getLogger(__name__)

class WarcHeaders(BaseModel):
    warc_record_id: Optional[str] = Field(alias='warc-record-id')
    warc_refers_to: Optional[str] = Field(alias='warc-refers-to')
    warc_date: str = Field(alias='warc-date')
    warc_block_digest: Optional[str] = Field(alias='warc-block-digest')
    warc_target_uri: str = Field(alias='warc-target-uri')
    warc_type: Optional[str] = Field(alias='warc-type')
    content_length: Optional[str] = Field(alias='content-length')
    warc_identified_content_language: Optional[str] = Field(alias='warc-identified-content-language')
    content_type: str = Field(alias='content-type')

class Record(BaseModel):
    content: str
    warc_headers: WarcHeaders
    metadata: Optional[Dict] = None

class HFFunctions:
    def __init__(self):
        self.fs = HfFileSystem()

        # Resume functionality
        self.resume = str(os.getenv("RESUME", "False")).lower() == "true"
        self.resume_file = os.getenv("RESUME_FILE", "resume.json")
        if self.resume:
            # create an empty resume file if it doesn't exist
            if not os.path.exists(self.resume_file):
                with open(self.resume_file, "w") as f:
                    json.dump({}, f)
    
    def get_sorted_folders(self) -> List[str]:
        """
        List all folders in the OSCAR community data directory and sort them by date (newest first).
        
        Returns:
            List of folder paths sorted by date (newest first)
        """
        try:
            # List all folders in the data directory
            folders = self.fs.ls("datasets/oscar-corpus/community-oscar/data", detail=False)
            
            # Filter out non-directory items if any
            folders = [f for f in folders if self.fs.isdir(f)]
            
            # Sort folders by date (newest first)
            # Folder names are expected to be in format like 2024-22
            sorted_folders = sorted(folders, reverse=True)
            
            logger.info(f"Found {len(sorted_folders)} folders, newest: {sorted_folders[0] if sorted_folders else 'None'}")
            return sorted_folders
        
        except Exception as e:
            logger.error(f"Error getting sorted folders: {e}")
            return []
    
    def get_ne_meta_files(self, folder_path: str) -> List[str]:
        """
        Get list of ne_meta/*.jsonl.zst files from a folder.
        
        Args:
            folder_path: Path to folder containing ne_meta directory
        
        Returns:
            List of ne_meta/*.jsonl.zst file paths
        """
        try:
            ne_meta_path = f"{folder_path}/ne_meta"
            if not self.fs.exists(ne_meta_path):
                logger.warning(f"ne_meta directory not found in {folder_path}")
                return []
            
            files = self.fs.ls(ne_meta_path, detail=False)
            jsonl_zst_files = [f for f in files if f.endswith('.jsonl.zst')]
            
            logger.info(f"Found {len(jsonl_zst_files)} jsonl.zst files in {ne_meta_path}")
            return jsonl_zst_files
        
        except Exception as e:
            logger.error(f"Error getting ne_meta files from {folder_path}: {e}")
            return []
    

    def download_file(self, file_name):
        """
        Download a file from the Hugging Face Hub.
        
        Args:
            file_name: Path to the file within the repo
            e.g. file_name = "data/2024-38/ne_meta/ne_meta_part_1.jsonl.zst"
            
        Returns:
            Local path to the downloaded file
        """
        logger.info(f"Downloading file: {file_name}")
        try:
            return hf_hub_download(repo_id='oscar-corpus/community-oscar', filename=file_name, repo_type="dataset")
        except Exception as e:
            logger.error(f"Error downloading file {file_name}: {e}")
            raise

    def yield_valid_records(self, file_path):
        """
        Generator function that yields valid records from a Zstandard-compressed JSON Lines file.
        
        Args:
            file_path: Path to the compressed file
            
        Yields:
            Valid Record objects
        """
        try:
            with open(file_path, 'rb') as compressed:
                dctx = zstd.ZstdDecompressor()
                with dctx.stream_reader(compressed) as reader:
                    text_stream = io.TextIOWrapper(reader, encoding='utf-8', errors='ignore')
                    for line_number, line in enumerate(text_stream, 1):
                        try:
                            data = json.loads(line)
                            record = Record(**data)
                            yield record
                        except (json.JSONDecodeError, ValidationError) as e:
                            # Skip invalid lines
                            logger.debug(f"Skipping invalid record at line {line_number}: {e}")
                            continue
        except Exception as e:
            logger.error(f"Error processing file {file_path}: {e}")
    
    def yield_rows(self):
        """
        Main function to process all files and yield valid records.
        
        Yields:
            Valid Record objects from all files
        """
        files = []

        folders = self.get_sorted_folders()
        logger.info(f"Found {len(folders)} folders to process.")

        for folder in folders:
            logger.info(f"Processing folder: {folder}")
            folder_files = self.get_ne_meta_files(folder)
            files.extend(folder_files)

        logger.info(f"Total number of files collected: {len(files)} e.g. {files[:2] if files else 'None'}")
        logger.debug(f"File list: {files}")
        
        if self.resume and os.path.exists(self.resume_file):
            try:
                with open(self.resume_file, "r") as f:
                    resume_data = json.load(f)
                    last_processed_file = resume_data.get("last_processed_file", None)
                    if last_processed_file:
                        try:
                            index = files.index(last_processed_file)
                            files = files[index+1:]  # Start from the next file
                            logger.info(f"Resuming from file after {last_processed_file}, {len(files)} files remaining")
                        except ValueError:
                            logger.warning(f"Last processed file {last_processed_file} not found in current file list. Starting from beginning.")
            except (json.JSONDecodeError, IOError) as e:
                logger.error(f"Error reading resume file: {e}. Starting from beginning.")
        
        for file_path in files:
            # Ensure the file path is in the correct format
            clean_path = file_path
            if "datasets/oscar-corpus/community-oscar/" in file_path:
                clean_path = file_path.replace("datasets/oscar-corpus/community-oscar/", "")

            try:
                download_path = self.download_file(clean_path)
                logger.info(f"Downloaded and processing file: {download_path}")

                valid_count = 0

                for record in self.yield_valid_records(download_path):
                    valid_count += 1
                    yield record

                logger.info(f"Processed file {file_path}: Found {valid_count} valid records")

                # Save processed file name for resume logic 
                if self.resume:
                    with open(self.resume_file, "w") as f:
                        json.dump({"last_processed_file": file_path}, f)
            
            except Exception as e:
                logger.error(f"Error processing file {file_path}: {e}")
                # Continue with next file instead of breaking the whole process
                continue


if __name__ == "__main__":
    hf_functions = HFFunctions()
    hf_functions.get_sorted_folders()
    files = []
    
    
    # print(hf_functions.get_sorted_folders())
    
    for folder in hf_functions.get_sorted_folders():
        print(f"\n\n Processing folder: {folder}")
        files.extend(hf_functions.get_ne_meta_files(folder))
        # print(files)
    print(f"len(files): {len(files)} : files: {files}")
    # len(files): 73 : files: ['datasets/oscar-corpus/community-oscar/data/2024-38/ne_meta/ne_meta_part_1.jsonl.zst', 'datasets/oscar-corpus/community-oscar/data/2024-38/ne_meta/ne_meta_part_2.jsonl.zst', 'datasets/oscar-corpus/community-oscar/data/2024-33/ne_meta/ne_meta_part_1.jsonl.zst', 'datasets/oscar-corpus/community-oscar/data/2024-33/ne_meta/ne_meta_part_2.jsonl.zst', 'datasets/oscar-corpus/community-oscar/data/2024-30/ne_meta/ne_meta_part_1.jsonl.zst', 'datasets/oscar-corpus/community-oscar/data/2024-30/ne_meta/ne_meta_part_2.jsonl.zst', 'datasets/oscar-corpus/community-oscar/data/2024-22/ne_meta/ne_meta_part_1.jsonl.zst', 'datasets/oscar-corpus/community-oscar/data/2024-22/ne_meta/ne_meta_part_2.jsonl.zst', 'datasets/oscar-corpus/community-oscar/data/2024-18/ne_meta/ne_meta_part_1.jsonl.zst', 'datasets/oscar-corpus/community-oscar/data/2024-18/ne_meta/ne_meta_part_2.jsonl.zst', 'datasets/oscar-corpus/community-oscar/data/2024-18/ne_meta/ne_meta_part_3.jsonl.zst', 'datasets/oscar-corpus/community-oscar/data/2023-50/ne_meta/ne_meta_part_1.jsonl.zst', 'datasets/oscar-corpus/community-oscar/data/2023-50/ne_meta/ne_meta_part_2.jsonl.zst', 'datasets/oscar-corpus/community-oscar/data/2023-50/ne_meta/ne_meta_part_3.jsonl.zst', 'datasets/oscar-corpus/community-oscar/data/2023-50/ne_meta/ne_meta_part_4.jsonl.zst', 'datasets/oscar-corpus/community-oscar/data/2023-23/ne_meta/ne_meta.jsonl.zst', 'datasets/oscar-corpus/community-oscar/data/2023-14/ne_meta/ne_meta.jsonl.zst', 'datasets/oscar-corpus/community-oscar/data/2023-06/ne_meta/ne_meta.jsonl.zst', 'datasets/oscar-corpus/community-oscar/data/2022-49/ne_meta/ne_meta.jsonl.zst', 'datasets/oscar-corpus/community-oscar/data/2022-40/ne_meta/ne_meta.jsonl.zst', 'datasets/oscar-corpus/community-oscar/data/2022-33/ne_meta/ne_meta.jsonl.zst', 'datasets/oscar-corpus/community-oscar/data/2022-27/ne_meta/ne_meta.jsonl.zst', 'datasets/oscar-corpus/community-oscar/data/2022-21/ne_meta/ne_meta_part_1.jsonl.zst', 'datasets/oscar-corpus/community-oscar/data/2022-21/ne_meta/ne_meta_part_2.jsonl.zst', 'datasets/oscar-corpus/community-oscar/data/2022-21/ne_meta/ne_meta_part_3.jsonl.zst', 'datasets/oscar-corpus/community-oscar/data/2022-05/ne_meta/ne_meta.jsonl.zst', 'datasets/oscar-corpus/community-oscar/data/2021-49/ne_meta/ne_meta.jsonl.zst', 'datasets/oscar-corpus/community-oscar/data/2021-43/ne_meta/ne_meta_part_1.jsonl.zst', 'datasets/oscar-corpus/community-oscar/data/2021-43/ne_meta/ne_meta_part_10.jsonl.zst', 'datasets/oscar-corpus/community-oscar/data/2021-43/ne_meta/ne_meta_part_11.jsonl.zst', 'datasets/oscar-corpus/community-oscar/data/2021-43/ne_meta/ne_meta_part_12.jsonl.zst', 'datasets/oscar-corpus/community-oscar/data/2021-43/ne_meta/ne_meta_part_2.jsonl.zst', 'datasets/oscar-corpus/community-oscar/data/2021-43/ne_meta/ne_meta_part_3.jsonl.zst', 'datasets/oscar-corpus/community-oscar/data/2021-43/ne_meta/ne_meta_part_4.jsonl.zst', 'datasets/oscar-corpus/community-oscar/data/2021-43/ne_meta/ne_meta_part_5.jsonl.zst', 'datasets/oscar-corpus/community-oscar/data/2021-43/ne_meta/ne_meta_part_6.jsonl.zst', 'datasets/oscar-corpus/community-oscar/data/2021-43/ne_meta/ne_meta_part_7.jsonl.zst', 'datasets/oscar-corpus/community-oscar/data/2021-43/ne_meta/ne_meta_part_8.jsonl.zst', 'datasets/oscar-corpus/community-oscar/data/2021-43/ne_meta/ne_meta_part_9.jsonl.zst', 'datasets/oscar-corpus/community-oscar/data/2021-39/ne_meta/ne_meta_part_1.jsonl.zst', 'datasets/oscar-corpus/community-oscar/data/2021-39/ne_meta/ne_meta_part_2.jsonl.zst', 'datasets/oscar-corpus/community-oscar/data/2021-39/ne_meta/ne_meta_part_3.jsonl.zst', 'datasets/oscar-corpus/community-oscar/data/2021-31/ne_meta/ne_meta.jsonl.zst', 'datasets/oscar-corpus/community-oscar/data/2021-25/ne_meta/ne_meta.jsonl.zst', 'datasets/oscar-corpus/community-oscar/data/2021-21/ne_meta/ne_meta_part_1.jsonl.zst', 'datasets/oscar-corpus/community-oscar/data/2021-21/ne_meta/ne_meta_part_2.jsonl.zst', 'datasets/oscar-corpus/community-oscar/data/2021-17/ne_meta/ne_meta.jsonl.zst', 'datasets/oscar-corpus/community-oscar/data/2021-10/ne_meta/ne_meta_part_1.jsonl.zst', 'datasets/oscar-corpus/community-oscar/data/2021-10/ne_meta/ne_meta_part_2.jsonl.zst', 'datasets/oscar-corpus/community-oscar/data/2021-10/ne_meta/ne_meta_part_3.jsonl.zst', 'datasets/oscar-corpus/community-oscar/data/2021-04/ne_meta/ne_meta_part_1.jsonl.zst', 'datasets/oscar-corpus/community-oscar/data/2021-04/ne_meta/ne_meta_part_2.jsonl.zst', 'datasets/oscar-corpus/community-oscar/data/2021-04/ne_meta/ne_meta_part_3.jsonl.zst', 'datasets/oscar-corpus/community-oscar/data/2020-50/ne_meta/ne_meta.jsonl.zst', 'datasets/oscar-corpus/community-oscar/data/2020-45/ne_meta/ne_meta.jsonl.zst', 'datasets/oscar-corpus/community-oscar/data/2020-40/ne_meta/ne_meta_part_1.jsonl.zst', 'datasets/oscar-corpus/community-oscar/data/2020-40/ne_meta/ne_meta_part_2.jsonl.zst', 'datasets/oscar-corpus/community-oscar/data/2020-40/ne_meta/ne_meta_part_3.jsonl.zst', 'datasets/oscar-corpus/community-oscar/data/2020-34/ne_meta/ne_meta.jsonl.zst', 'datasets/oscar-corpus/community-oscar/data/2020-29/ne_meta/ne_meta.jsonl.zst', 'datasets/oscar-corpus/community-oscar/data/2020-24/ne_meta/ne_meta.jsonl.zst', 'datasets/oscar-corpus/community-oscar/data/2020-16/ne_meta/ne_meta.jsonl.zst', 'datasets/oscar-corpus/community-oscar/data/2020-10/ne_meta/ne_meta.jsonl.zst', 'datasets/oscar-corpus/community-oscar/data/2020-05/ne_meta/ne_meta.jsonl.zst', 'datasets/oscar-corpus/community-oscar/data/2019-22/ne_meta/ne_meta.jsonl.zst', 'datasets/oscar-corpus/community-oscar/data/2018-47/ne_meta/ne_meta.jsonl.zst', 'datasets/oscar-corpus/community-oscar/data/2018-30/ne_meta/ne_meta.jsonl.zst', 'datasets/oscar-corpus/community-oscar/data/2017-43/ne_meta/ne_meta.jsonl.zst', 'datasets/oscar-corpus/community-oscar/data/2017-13/ne_meta/ne_meta.jsonl.zst', 'datasets/oscar-corpus/community-oscar/data/2016-22/ne_meta/ne_meta.jsonl.zst', 'datasets/oscar-corpus/community-oscar/data/2015-48/ne_meta/ne_meta.jsonl.zst', 'datasets/oscar-corpus/community-oscar/data/2015-14/ne_meta/ne_meta.jsonl.zst', 'datasets/oscar-corpus/community-oscar/data/2014-42/ne_meta/ne_meta.jsonl.zst']

    # get data for files[:2]
    for file_path in files[:2]:
        if "datasets/oscar-corpus/community-oscar/" in file_path:
                file_path = file_path.replace("datasets/oscar-corpus/community-oscar/", "")
        
        # download file
        download_path = hf_functions.download_file(file_path)

        print(f"processing file: {download_path}")
        # processing file: /root/.cache/huggingface/hub/datasets--oscar-corpus--community-oscar/snapshots/5b848c33d6cb8d15a0544650508d7c6a63d9b747/data/2024-38/ne_meta/ne_meta_part_2.jsonl.zst

        count = 0

        for _ in hf_functions.yield_valid_records(download_path):
            valid_count += 1

        print(f"Total valid rows: {valid_count}")
        # Total valid rows: 1452339
        
        # print(f"example row: {_}")
        # example row: content='न्यायाधीशहरूलाई प्रश्न– भ्रष्टाचारमा निलम्बित सांसदलाई सर्वोच्चले कसरी दियो उन्मुक्ति? :: मनोज सत्याल :: Setopati\nराजनीति\nSubscribe Setopati\nशुक्रबार, भदौ २८, २०८१युनिकोडENEnglish\nगृहपृष्ठ\nप्रिमियम स्टोरी\nराजनीति\nबजार अर्थतन्त्र\nविचार\nनेपाली ब्रान्ड\nसमाज\nकला\nब्लग\nखेलकुद\nग्लोबल\nकभर स्टोरी\nन्यायाधीशहरूलाई प्रश्न– भ्रष्टाचारमा निलम्बित सांसदलाई सर्वोच्चले कसरी दियो उन्मुक्ति?\nमनोज सत्याल\nकाठमाडौं, चैत १८\nसर्वोच्च अदालतले शुक्रबार गरेको एउटा फैसलाले विशेष अदालतबाट भ्रष्टाचारी ठहर भएका सांसद टेकबहादुर गुरूङलाई कानुन निर्माण गर्ने भूमिकामा पुर्\u200dयाइदिएको छ।\nअर्थात् भ्रष्टाचारी ठहर भएका व्यक्तिले अब सुशासन र कानुनी व्यवस्थाका लागि कानुन बनाउने छन्।\nयति मात्र होइन, भ्रष्टाचारी ठहर भएको व्यक्तिले अब राज्य कोषबाट नियमित तलब भत्ता समेत पाउने छन्। उनलाई सेना र प्रहरीले सलाम ठोक्ने छ। अनि अख्तियार दुरूपयोग अनुसन्धान आयोग जसले सांसदविरूद्ध भ्रष्टाचारको मुद्दा चलायो, अब ती सांसदले नै अख्तियारका आयुक्तविरूद्ध महाभियोग समेत ल्याउन सक्छन्।\nमनाङबाट निर्वाचित कांग्रेसका सांसद गुरूङलाई भ्रष्टाचार अभियोगमा सांसद पदबाट गरिएको निलम्बन सर्वोच्च अदालतले खारेज गरिदिएसँगै उक्त फैसलामा कयौं प्रश्न खडा भएका छन्। भ्रष्टाचारमा एउटा अदालतबाट दोषी ठहर भएको र अर्को अदालतमा मुद्दा विचाराधीन भएको व्यक्ति सांसदको लोगो लगाएर संसद बैठकमा जाने अवस्था बनेको छ। उनै व्यक्तिले संसदीय समितिमा बसेर कानुन बनाउने र सरकारको निगरानी गर्ने अवस्था बनेको छ।\nसांसद गुरूङलाई २०७९ फागुन १५ गते विशेष अदालतले काठमाडौंको फनपार्कको ६० रोपनी जग्गा सस्तो भाडादरमा सञ्चालन गर्न अनुमति दिएर भ्रष्टाचार गरेको मुद्दामा दोषी ठहर गरेको थियो। लगत्तै अदालतले २०७९ फागुन २५ गते उनलाई एक करोड २१ लाख ८४ हजार रूपैयाँ जरिवाना र सो बराबरको बिगो असुल उपर गर्ने निर्णय गरेको थियो।\nतर सर्वोच्चले कानुनी छिद्रहरू प्रयोग गरेर गुरूङलाई संसद जाने बाटो खोलिदिएको हो।\nभ्रष्टाचार मुद्दामा एउटा अदालतबाट दोषी ठहर भएको व्यक्तिले सांसदको हैसियतमा काम गर्न मिल्छ कि मिल्दैन भन्ने विषयमा संवैधानिक व्याख्या गर्नुको साटो सर्वोच्चले प्राविधिक व्याख्या गरेर गुरूङलाई कानुनी छिद्रता उपयोग गर्ने बाटो दिएको हो।\nजबकि भ्रष्टाचार निवारण ऐन २०५९ को दफा ३३ ले भ्रष्टाचारको मुद्दा लाग्दाको अवस्थामा समेत सांसदहरू पदबाट निलम्बन हुने व्यवस्था गरेको छ। मुद्दा लाग्दा मात्र पनि पदबाट निलम्बन हुने व्यक्तिलाई सर्वोच्चले भ्रष्टाचारमा दोषी ठहर हुँदा समेत सांसदका रूपमा काम गर्न सक्ने स्थिति निर्माण गरिदिएको हो।\nसर्वोच्चका न्यायाधीशद्वय मनोजकुमार शर्मा र विनोद शर्माको संयुक्त इजलासले भ्रष्टाचार जस्तो नैतिक पतन देखिने गम्भीर फौजदारी मुद्दाको व्याख्या गर्नुको साटो प्राविधिक र कानुनी छिद्रमा टेकेर गुरूङको मुद्दामा फैसला गरेका छन्।\nसर्वोच्चले गुरूङको निलम्बन खारेज गर्दा संविधानको धारा २० (५), ८७, २४६ (३), संघीय संसद सचिवालय सम्बन्धी ऐन २०६४ को दफा ९, प्रतिनिधि सभा नियमावली २०७९ को नियम २४८ (३) र (४) को प्रयोग र व्याख्या त्रुटिपूर्ण ढंगबाट गरिएको उल्लेख गरेको छ। तर अदालतले जारी गरेको संक्षिप्त आदेशमा कुन धारा र दफाको कसरी त्रुटिपूर्ण प्रयोग भयो भन्ने व्याख्या गरेको छैन। फैसलाको पूर्णपाठ आउन बाँकी छ।\nअदालतले गुरूङको निलम्बन खारेज गर्न सहायता लिएको पहिलो संविधानको धारा हो – २० को उपधारा ५।\nधारा २० मा न्याय सम्बन्धी हकको व्यवस्था छ। उक्त धाराको उपधारा (५) मा \'कुनै अभियोग लागेको व्यक्तिलाई निजले गरेको कसुर प्रमाणित नभएसम्म कसुरदार मानिने छैन\' भन्ने उल्लेख छ।\nटेकबहादुर गुरूङको हकमा उनलाई विशेष अदालतमा मुद्दा दायर भई उनी दोषी ठहर भएको अवस्था छ। उनलाई विशेष अदालतले कैद सजाय नगरे पनि जरिवाना गरेको अवस्था छ। यद्यपि गुरूङ विशेष अदालतको फैसलाविरूद्ध २०८० असोज १ गते पुनरावेदनमा गएको स्थिति छ।\nसर्वोच्चको फैसला हेर्दा विशेष अदालतबाट भएको कसुर प्रमाणित भएको अर्थका रूपमा नलिएको देखिन्छ। बरू सर्वोच्चमा गुरूङले विशेष अदालतको फैसलाविरूद्ध गरेको पुनरावेदन विचाराधीन भएकाले कसुरदार मान्न नमिल्ने व्याख्या गरेको देखिन्छ।\nदोस्रो सर्वोच्चले गुरूङको निलम्बन खारेज गर्न संविधानको धारा ८७ लाई कारण बनाएको छ। संविधानको धारा ८७ मा संघीय सांसद बन्न आवश्यक योग्यताबारे उल्लेख छ। धारा ८७ ले कुनै पनि व्यक्ति सांसद बन्न – नेपाली नागरिक हुनुपर्ने, प्रतिनिधि सभाको हकमा २५ वर्ष उमेर पुगेको र राष्ट्रिय सभाको हकमा ३५ वर्ष उमेर पुगेको हुनुपर्ने, नैतिक पतन देखिने फौजदारी कसुरमा सजाय नपाएको हुनुपर्ने, कुनै संघीय कानुनले अयोग्य नभएको र कुनै लाभको पदमा बहाल रहेको हुन नहुने व्यवस्था गरेको छ।\nसर्वोच्चले गुरूङको मुद्दामा धारा ८७ उल्लेख गरे पनि यसको व्याख्या गरेको छैन। जबकि धारा ८७ उपधारा १ को (ग) मा नैतिक पतन देखिन फौजदारी कसुरमा सजाय नपाएको हुनुपर्ने भनेको छ।\nविशेष अदालतबाट भ्रष्टाचारी ठहर भएका सांसद गुरूङ कसरी नैतिक पतन देखिने फौजदारी कसुरमा सजाय नपाएको र सांसद बन्न योग्य छन् भनेर सर्वोच्चले अर्थ्याउँछ, त्यो भने हेर्न बाँकी छ।\nसर्वोच्चले गुरूङको निलम्बन खारेजी गर्दा टेकेको अर्को धारा हो – २४६ (३)।\nउक्त धारामा निर्वाचन आयोगको काम कर्तव्य र अधिकार सम्बन्धी व्यवस्था छ। उपधारा ३ मा \'राष्ट्रपति, उपराष्ट्रपति, संघीय संसदका सदस्य, प्रदेश सभा सदस्य वा स्थानीय तहका सदस्यका लागि उम्मेदवारीको मनोनयन दर्ता भइसकेको तर निर्वाचन परिणाम घोषणा भई नसकेको अवस्थामा कुनै उम्मेदवारको योग्यता सम्बन्धमा कुनै प्रश्न उठेमा त्यसको निर्णय निर्वाचन आयोगले गर्नेछ\' भन्ने व्यवस्था छ।\nगुरूङको हकमा अदालतले यो धारालाई निर्वाचन लड्न योग्य भएपछि सांसद बन्न पनि योग्य हुन्छ भन्ने ढंगले अर्थ्याउन खोजेको देखिन्छ।\nटेकबहादुर गुरूङविरूद्ध विशेष अदालतमा २०७५ माघ २१ गते मुद्दा दायर गरिएको थियो। उनको मुद्दामा २०७९ फागुन १६ गते मात्र विशेष अदालतबाट फैसला भएको थियो।\nभ्रष्टाचारको मुद्दा विचाराधीन रहँदा नै गुरूङ त्यसबीच २०७९ मंसिर ४ गते भएको निर्वाचनमा कांग्रेसबाट उम्मेदवार बने। उनीविरूद्ध प्रतिस्पर्धी उम्मेदवार एमालेका पाल्देन छोपाङ गुरूङले मुख्य निर्वाचन अधिकृत उत्तम सिलवाल समक्ष उम्मेदवारी खारेजीको माग राख्दै उजुरी पनि दिएका थिए। तर न्यायाधीश समेत रहेका सिलवालले २०७९ असोज २५ गते निर्वाचन ऐन २०७४ को दफा १३ (घ) अनुसार \'भ्रष्टाचार मुद्दामा सजाय पाएको अवस्थामा मात्र उम्मेदवारको अयोग्यता कायम हुने\' व्यवस्था गरेकाले प्रस्तुत उजुरी तथा संलग्न कागजातबाट टेकबहादुर गुरूङविरूद्ध भ्रष्टाचार मुद्दा दायर भइ फैसला नभइसकेको देखिन्छ। अतः उक्त कानुनी व्यवस्था अन्तिम नभएकाले उजुरी खारेज\' हुने निर्णय गरेका थिए।\nन्यायाधीशद्वयको संक्षिप्त आदेशमा समेत सांसद गुरूङको भ्रष्टाचार मुद्दा विचाराधीन रहेकाले अयोग्यता सिर्जना नहुने भनी मुख्य निर्वाचन अधिकृतले गरेको व्याख्या समेत उल्लेख छ। तर निर्वाचन अधिकृतले दोषी ठहर नहुँदा गरेको निर्णयलाई अहिले सर्वोच्चले दोषी ठहर भइसकेको अवस्थामा व्याख्या गरेको छैन। अझ भनौं, व्याख्याको लेठो गर्न खोजेको देखिँदैन। बरू न्यायाधीशद्वयले गुरूङले विशेष अदालतको फैसलाविरूद्ध सर्वोच्चमा पुनरावेदन गरेकाले मुद्दा विचराधीन रहेको उल्लेख गरिदिएका छन्।\nसर्वोच्च अदालतले टेकेको अर्को कानुन हो – संघीय संसद सचिवालय सम्बन्धी ऐन २०६४ को दफा ९।\nऐनको दफा ९ मा संघीय संसदका महासचिवको काम, कर्तव्य र अधिकार सम्बन्धी व्यवस्था छ।\nटेकबहादुर गुरूङ सांसद पदबाट निलम्बन भएको सूचना संघीय संसदका तत्कालीन महासचिव भरतप्रसाद गौतमले २०७९ पुस १२ गते प्रकाशन गरेका थिए।\nसूचनामा गुरूङविरूद्ध विशेष अदालतमा भ्रष्टाचार मुद्दा विचाराधीन रहेको भनेर अख्तियारबाट २०७९ पुस ८ गते पत्र प्राप्त भएकाले भ्रष्टाचार निवारण ऐनको दफा ३३ अनुसार निलम्बन हुने उल्लेख छ।\nतर सर्वोच्चले संसदको महासचिवलाई यस्तो निलम्बनको सूचना प्रकाशित गर्ने अधिकार नरहेको भन्ने ढंगबाट उक्त कानुनको दफा ९ समातेको देखिन्\u200dछ। दफा ९ मा महासचिवको काम कर्तव्यमा यसरी सांसदको निलम्बनको सूचना प्रकाशित गर्ने लगायत विषय उल्लेख छैनन्। संघीय संसदले भने विगतदेखि नै अर्को निकायबाट लेखिआएको र भ्रष्टाचार निवारण ऐनमा स्पष्ट रूपमा उल्लिखित विषयमा महासचिवद्वारा सूचना प्रकाशन गर्दै आएको छ।\nउदाहरणका लागि, २०७६ माघ २४ गते कांग्रेस सांसद विजयकुमार गच्छदारविरूद्ध ललिता निवास प्रकरणमा विशेष अदालतमा मुद्दा दायर भएपछि स्वतः निलम्बन भएको सूचना निमित्त महासचिव गोपालनाथ योगीले प्रकाशित गरेका थिए। उक्त पत्रमा भ्रष्टाचार निवारण ऐन अनुसार स्वतः निलम्बन हुने व्यवस्था भएको उल्लेख छ।\nसर्वोच्चले भने यसरी अन्य निकायबाट लेखिआएको र भ्रष्टाचार निवारण ऐनले स्पष्ट पारेको विषयलाई महासचिवको प्रशासनिक कामका रूपमा लिएको छैन। बरू गुरूङको निलम्बन खारेजी गर्दा महासचिवको काम कर्तव्य र अधिकार उल्लेख भएको दफा देखाउँदै अधिकार क्षेत्रबाहिर गएको ढंगबाट अर्थ्याउन खोजेको छ।\nसर्वोच्चले गुरूङको निलम्बन खारेजी गर्दा प्रतिनिधि सभा नियमावलीको नियम २४८ को (३) र (४) को विषयलाई पनि समेटेको छ। नियमावलीको नियम २४८ मा सांसद पक्राउ परेको जानकारी संसदलाई दिनुपर्ने व्यवस्था छ।\nनियम (३) मा \'तीन वर्ष वा सोभन्दा बढी कैदको सजाय हुने वा नैतिक पतन देखिने फौजदारी मुद्दामा अभियोग पत्र दायर भइ निज पुर्पक्षका लागि थुनामा रहेकामा त्यस्तो थुनामा रहेको अवधिभर वा अदालतको आदेश बमोजिम पुर्पक्षका लागि थुनामा बस्नुपर्नेमा, त्यस्तो थुनामा नबसी फरार रहेको भएमा, निजलाई सदस्यको हैसियतले कुनै कार्य गर्न वा कुनै अधिकार वा उन्मुक्ति प्राप्त हुने छैन र त्यस्तो अवधिभर निज निलम्बनमा रहने र पारिश्रमिक, सेवा र सुविधा समेत स्थगित हुने\' व्यवस्था छ।\nअर्थात् तीन वर्षभन्दा बढी सजाय हुने वा नैतिक पतन देखिने फौजदारी अभियोग पत्र दायर भएर थुनामा रहेमा मात्र सांसद निलम्बन रहनेछ।\nटेकबहादुर गुरूङको हकमा अभियोगपत्र दायर भए पनि उनी थुनामा थिएनन्। विशेष अदालतले उनलाई भ्रष्टाचारी ठहर गर्दा जरिवाना र बिगो पनि तोकेको छ। तर उनलाई कैद सजाय गरिएको छैन।\nप्रतिनिधि सभा नियमावलीको नियम (३) को स्पष्टीकरणमा \'अदालत भन्नाले जिल्ला अदालत, उच्च अदालत र सर्वोच्च अदालतलाई सम्झनुपर्ने छ\' भन्ने उल्लेख छ। अर्थात् यो नियमले सिधै विशेष अदालतको उपस्थितिलाई नै अस्वीकार गरेको देखिन्छ।\nप्रतिनिधि सभाको यस्तो नियमावली सांसदहरू आफैंले तयार पार्छन् र आफैं पारित गर्छन्। नियमावली एक ढंगले कानुन भन्दा पनि सांसदहरूले आफ्नो दैनिक कामकारबाही सञ्चालन गर्न तयार गरेको स्वनियम हो। यसरी सांसदहरू आफैंले बनाएको नियममा अख्तियार जस्तो निकायले मुद्दा दर्ता गर्ने विशेष अदालतलाई \'बाइपास\' गरेका छन्। गुरूङको मुद्दाको हकमा सर्वोच्चले प्रतिनिधि सभा नियमावलीको नियम (३) उल्लेख गर्नुको अर्थ विशेष अदालतको निर्णयको औचित्य नरहेको रूपमा व्याख्या गर्न खोजेको देखिन्छ।\nसंसद सचिवालय भने भ्रष्टाचार निवारण ऐन अनुसार सांसद गुरूङ निलम्बनमा भएकाले प्रतिनिधि सभा नियमावलीको जिकिर उचित नहुने बुझाइमा छ। यसबीच अदालतबाट नैतिक पतन देखिने भ्रष्टाचार मुद्दामा दोषी ठहर भएर जेल जान नपरेका गुरूङको निलम्बन संसद सचिवालयले हटाएको थिएन।\nभ्रष्टाचार मुद्दा दायर मात्र हुँदा निलम्बित हुने सांसद दोषी ठहर हुँदा निलम्बन हटाउन संसद सचिवालय तयार थिएन। कसैले लेखेर पठाएको पनि थिएन। गुरूङ स्वयं पनि कहिल्यै संसद सचिवालयमा आफूलाई बैठकमा सहभागी गराउन वा हाजिर गराउन माग लिएर गएनन्।\n\'उहाँ सिधै अदालत जानुभयो। उहाँलाई निलम्बन पनि भ्रष्टाचार निवारण ऐन अनुसार, अख्तियारको पत्र अनुसार भएको हो। संसदको नियमावली अनुसार हो,\' संघीय संसदका प्रवक्ता एकराम गिरीले भने, \'सर्वोच्चले उहाँ निलम्बनमा नपर्ने भनेर फैसला गरेको समाचारबाट थाहा पाएका छौं। फैसला आएको छैन। तर उहाँ कहिल्यैप नि मलाई हाजिर गराऊ भनेर आउनुभएको हाम्रो जानकारीमा छैन।\'\nसंसद सचिवालयका प्रवक्ता गिरी अदालतको फैसला प्राप्त भएपछि कार्यान्वयन गर्नु दायित्व भएको बताउँछन्।\nत्यस्तै अदालतले प्रतिनिधि सभा नियमावलीको नियम २४८ उपनियम (४) लाई पनि उल्लेख गरेको छ। उपनियम ४ मा \'कुनै सदस्यलाई फौजदारी मुद्दामा अदालतको फैसलाले कैदको सजाय हुने ठहर भएकामा त्यस्तो कैदमा बस्नुपर्ने अवधिभर वा कुनै सदस्यले फौजदारी मुद्दामा अदालतको फैसला बमोजिम कैदको सजाय भुक्तान गरिरहेकामा सो अवधिभर वा कुनै सदस्यले कैद भुक्तान गर्न बाँकी रहेमा वा पक्राउ परी कैद भुक्तान नगरेसम्मको अवधिभर त्यस्तो सदस्य स्वतः निलम्बन भएको मानिने\' उल्लेख छ।\nनियमावलीको यो व्यवस्था अनुसार विशेष अदालतबाट कैद सजाय नपाएका गुरूङ कैदमा नभएकाले संसदमा जान मिल्ने व्याख्या सर्वोच्चले गर्न खोजेको देखिन्छ। नियमावलीले स्पष्ट रूपमा फौजदारी मुद्दामा कैदमा बस्नुपर्ने भए सो अवधिधर निलम्बन हुने व्यवस्था गरेको छ। यसरी सांसदहरू आफैंले बनाएको नियमावलीले गुरूङको हकमा उन्मुक्तिको अवस्था सिर्जना गरेको देखिन्छ। भ्रष्टाचारमा दोषी ठहर हुँदाहुँदै पनि गुरूङलाई कैद सजाय नभएकाले उनी अब संसद बैठकमा जान पाउने भएका छन्। सर्वोच्चले पनि नियमावलीको यही व्यवस्था संघीय संसद सचिवालयलाई सम्झाएको छ।\nसर्वोच्चले यसरी विशेष अदालतबाट भ्रष्टाचारी ठहर भएका गुरूङको निलम्बन हटाएर संसद जाने बाटो खोलिदिएको छ। संविधान, कानुन र नियमावली व्यवस्थाको व्याख्या गरे पनि सर्वोच्चले जुन कानुन अनुसार गुरूङ निलम्बन भएका हुन्, उक्त कानुनबारे केही बोलेको छैन। भ्रष्टाचार निवारण ऐन अनुसार निलम्बन भएका गुरूङको हकमा उक्त कानुनबारे सर्वोच्चले उल्लेख समेत गरेको छैन।\nत्यसैले सर्वोच्चको यो व्याख्यामाथि प्रश्न उठ्ने अवस्था बनेको छ।\nसर्वोच्चका पूर्वन्यायाधीश बलराम केसी यो फैसलामा असहमत छन्।\n\'हामीले फैसला मान्नुपर्छ तर यसमा असहमत हुन पाइन्छ,\' केसीले भने, \'म यो फैसलामा असहमत छु। सरकारले महान्यायाधिवक्तामार्फत् पुनरावलोकनका लागि जानुपर्छ। यस्तो फैसला कायम रहनु हुँदैन।\'\nकेसी यो फैसलाले तल्लो स्तरको अपराध गर्ने मान्छेलाई पदमा बस्ने बाटो खोलेको बताउँछन्।\n\'यसले हाम्रो प्रजातान्त्रिक सुसंस्कारको व्यवस्था बिगार्छ। कानुनको असमान प्रयोगको अवस्था बनाउँछ,\' केसीले भने, \'सांसदलाई एउटा र सरकारी कर्मचारी खरिदार, सुब्बालाई अर्को कानुन हुनु हुँदैन। भ्रष्टाचार प्रधानमन्त्रीले गरे पनि, पियनले गरे पनि एउटै हो। प्रजातन्त्रमा समान व्यवहार हुनुपर्छ। सरकार यसको पुनरावलोकनमा जानुपर्छ।\'\nपूर्वन्यायाधीश केसी विशेष अदालतको फैसला कार्यान्वयनमा गइसकेको अवस्थामा सर्वोच्चको फैसलामा कमी देख्छन्।\n\'दण्डनीय कानुनमा अक्षरमा जे लेखिएको छ, त्यही अनुसार व्याख्या गर्नुपर्छ। जब उसलाई (टेकबहादुर गुरूङलाई) विशेष अदालतले सजाय गर्दा र अहिले त्यसविरूद्ध पुनारावेदन गर्दाको बीचमा दण्डनीय कानुन आकर्षित हुन्छ,\' पूर्वन्यायाधीश केसीले भने, \'संसदले यस्तो भन्न चाहेको होला भन्न पाइँदैन। अहिले दण्डनीय कानुन विशेष अदालतबाट प्रयोग भइसकेको अवस्था छ। उनी ठीक भएन भनेर पुनरावेदनमा गएका छन्। अब संसदले यस्तो भन्न चाहेको होला भनेर भन्न पाइँदैन। यस्तो भनेको रहेछ भनेर लेखेको अक्षरबाट पत्ता लगाउनुपर्छ। अहिले दण्डनीय कानुन अनुसार सजाय भइसकेपछि पदमा बस्न नैतिकताले मिल्ने/नमिल्ने, कानुनले मिल्ने/नमिल्ने कुरा छ। यस्तो अवस्थामा मेरो विचारमा सर्वोच्चले कानुनी शासन र सुशासनको पक्षमा फैसला गर्नुपर्छ।\'\nकेसीले थपे, \'अब २७५ जना सांसदका बीच एक जना भ्रष्टाचारको मुद्दा चलेको व्यक्ति बस्ने कि नबस्ने भन्ने प्रश्न उठ्छ। भोलि अधिकांश त्यस्ता सांसद भए भने के गर्ने। अनि त्यस्ता मान्छेले बनाएको कानुन समाजमा के हुन्छ?\'\nगुरूङले सर्वोच्चमा गरेको पुनारावेदनबाट शुद्धीकरण नहुँदासम्म संसदले पर्खिनुपर्ने केसी बताउँछन्।\n\'अहिले प्रश्न भनेको संसदमा पद त्यो व्यक्ति योग्य हुने वा नहुने भन्ने हो,\' केसीले भने, \'अदालतले शुद्धीकरण गरेपछि मात्र संसदमा त्यस्तो व्यक्ति फिट हुन्छ भन्ने सिद्धान्तमा हामी जानुपर्छ।\'\nसंविधानविद भीमार्जुन आचार्य संवैधानिक शास्त्रमा फैसलाको स्थिरता हुनुपर्ने बताउँछन्।\n\'फैसला स्थिरताको सिद्धान्तले पहिले भएका निर्णय र फैसलालाई पछि आउने न्यायाधीशहरूले सम्मान गर्नुपर्ने, फैसलाहरूमा एकरूपता हुनुपर्ने र स्थिरता हुनुपर्ने भन्छ,\' आचार्यले भने, \'विगतमा सांसद हरिनारायण रौनियार यही माग गरेर सर्वोच्चमा गएका थिए। उनको रिट खारेज भयो। निलम्बन गरेको ठीक छ भनियो। अहिले टेकबहादुरको निलम्बन फुकुवा गरिएको छ। अलिकति फैसलाको बीचमा स्थिरता र एकरूपता देखिएन।\'\nअदालतले अन्याय भएको व्यक्तिलाई न्याय दिने संवैधानिक दायित्व हुने उनी बताउँछन्।\n\'तर छिद्रहरू खोजेर कहाँबाट प्रवेश गर्ने अनि जबरजस्ती तर्क मिलाएर निर्णय दिने कुरा अदालतको न्यायिक धर्मभित्र पर्दैन,\' आचार्यले भने, \'अहिलेको हाम्रो कानुनमा सार्वजनिक पद धारण गरेको व्यक्ति उपर भ्रष्टाचारको मुद्दा दर्ता हुनेबित्तिकै निलम्बन हुने भन्नेछ। अब सांसदलाई फरक र अन्यलाई फरक गर्ने हो भने संविधानदेखि नै सुधार गर्नुपर्ने हुन्छ। अदालतको फैसला हतार भयो कि जस्तो लाग्छ।\'\nकानुनले भ्रष्टाचारमा दोषी ठहर भएको व्यक्ति संसदमा जाने परिकल्पना नगर्ने आचार्यको जिकिर छ।\n\'यस्तोमा न्यायमूर्तिहरूले ख्याल गर्नुपर्छ,\' आचार्यले भने, \'नयाँ व्याख्या नहुने त हुँदैन तर फैसला पढ्दा नयाँ व्याख्याका आधारहरू देखिँदैनन्।\'\nप्रकाशित मिति: आइतबार, चैत १८, २०८० १९:४५\nसिफारिस\nकोदोको \'प्रिमियम कुकिज\' ब्रान्डिङ गर्दै पशुपति\nक्षमताभन्दा बढी कैदीबन्दी हुँदा खोटाङ कारागारमा समस्या\nपहिरोले भीमदत्त र केआईसिंह राजमार्ग पूर्ण अवरुद्ध\nयी तीन प्रदेशमा धेरै भारी वर्षा हुने पूर्वानुमान\nपानी पर्दा हिलाम्य, घाम लाग्दा धुलाम्य सडक (भिडिओ)\nडेटिङ एपबाट दोस्ती बढाएर लगानी गर्न लगाउँथे क्रिप्टोमा\nसम्भव भएसम्म रिक्त सबै संवैधानिक पदमा नियुक्ति सिफारिस गर्न प्रचण्डको आग्रह\nनिर्देशक दीपेन्द्र भन्छन्- उजेली जत्तिको फिल्म बनाउन सक्दिनँ कि!\nबालुवाटारमा देउवालाई ओलीको दोसल्ला\nप्रतिक्रिया दिनुहोस्\nथप\nराजनीति\nमाओवादी संसदीय दलको बैठक बस्दै, यी हुन् एजेन्डा\nरास्वपा प्रमुख सचेतकलाई प्रश्न– उपसभामुखलाई किन नहटाउने? (भिडिओ)\nकोइराला पक्षलाई बाहिरै राखेर कोशी सरकारमा गयो कांग्रेस\nमेरो उमेर पनि गयो, रिटायरमेन्टको बेला भइसकेको छ: देउवा\nक्षमताभन्दा बढी कैदीबन्दी हुँदा खोटाङ कारागारमा समस्या\nएमाले सचिवालय बैठक बस्दै\nविचार\nविश्वको ध्यान आकर्षित गर्ने चीनको त्यो बैठकचेतनाथ आचार्य\nस्वास्थ्य सेवामा गरिबको पहुँच कसरी बढाउने?डा. राजु लौडारी\nनिजामती सेवा विधेयकमा चाहिएका सुधार र परिमार्जनराजन भट्टराई\nएसइईको आधा आकाश किन अँध्यारो?डा. नवराज खतिवडा\nब्लग\nमरुभूमिबाट मातृभूमि फर्किँदा!डिल्लीप्रसाद कँडेल\nलेखेर पनि बाँचिन्छ र?कान्ति न्यौपाने\nके सिटिइभिटीमा विद्यार्थी घटेका हुन्, के भन्छ तथ्यांक?वसन्त आचार्य\nपरदेशीको कथा, दुई छाकको व्यथा!सृजना उप्रेती\nसाहित्यपाटी\n\'तिमीले बिहे गरिसक्यौ होला है?\'रामचन्द्र अधिकारी\nखोज्दै छु म हराएको आफू भित्रको मान्छेलाई!उत्तम मगर\nमलाई ठुलो अफर नआओस्!मित्र पाठक `काँचुली`\nमलाई आराम गर्ने अधिकार पनि रहेनछ! सुस्मिता खतिवडा\nकेटाकेटीका कुरा\n\'जापानमा पनि नेपालकै याद आउँछ\'स्यरोन भुर्तेल\nमेरो झोलाबाट हराएको फोनकरूणा मगर\nगोरखामा एक दिनउज्ज्वल ढकाल\n\'भविष्यका पाहुना\' मा एलन मस्कको जीवनीसुश्रुतराज न्यौपाने\nपाठक विचार\nत्रिवि सेवा आयोगलाई एक कर्मचारीको निवेदन- फेल हुन पाऊँ!बिदुर दाहाल\nसूचनापाटी\nयुनिकोडमा टाइप गर्नुहोस्\nविनिमय दर\nशेयर बजार\nसुन चाँदि\nरेडियो सुन्नुहोस्\nसम्पर्क\nSetopati Sanchar Pvt. Ltd. सूचना विभाग दर्ता नंः १४१७/०७६-२०७७ Jhamsikhel Lalitpur, Nepal\n01-5429319, 01-5428194 setopati@gmail.com विज्ञापनका लागि 015544598, 9801123339, 9851123339\nसोसल मिडिया\nLike us on FacebookFollow us on TwitterSubscribe YouTube ChannelFollow us on InstagramFollow us on Tiktok\nसेतोपाटी\nगृहपृष्ठ\nविनिमय दर\nशेयर बजार\nसुन चाँदि\nहाम्रोबारे\nगोपनीयता नीति\nप्रधान सम्पादक\nअमित ढकाल\nसेतोपाटी टीम\nहाम्रो टीम\n© 2024 Setopati Sanchar Pvt. Ltd. All rights reserved. Site by: SoftNEP\nHi! `+res.first_name+``; $(".login-link").addClass("active"); $(\'#login-container\').html(loginContainer); } }, error: function(res){ if(res.status == 403){ var loginContainer = `Log In`; $(\'#login-container\').html(loginContainer); } } }); } checkAuth(); });' warc_headers=WarcHeaders(warc_record_id='<urn:uuid:a7f2a80f-e61a-41d4-97f9-891d627d2675>', warc_refers_to='<urn:uuid:537c8eb1-e1af-409e-a271-08a3d78b76a8>', warc_date='2024-09-13T07:31:31Z', warc_block_digest='sha1:3ARAAVGHWD4HENMWOFARX7DHI7MPFBXF', warc_target_uri='https://www.setopati.com/politics/326081', warc_type='conversion', content_length='44776', warc_identified_content_language='nep', content_type='text/plain') metadata={'identification': {'label': 'ne', 'prob': 0.8741406}, 'harmful_pp': 10687.927, 'tlsh': 'tlsh:T1E8135F139054412BAA30ED6A5DF710B7B4E339D097EF12C80ABE20F53D3665E278BD66B2C34DFB78F3655AEC7897364C0C36D9B8267083B5525788E4F3D35881CB4019A6A2', 'quality_warnings': ['short_sentences', 'header', 'footer'], 'categories': None, 'sentence_identifications': [None, None, None, None, {'label': 'hi', 'prob': 0.91412675}, {'label': 'hi', 'prob': 0.9839729}, None, None, {'label': 'mr', 'prob': 0.839196}, None, None, {'label': 'hi', 'prob': 0.93576795}, {'label': 'hi', 'prob': 0.95742697}, {'label': 'ne', 'prob': 0.99247843}, {'label': 'hi', 'prob': 0.9492401}, {'label': 'hi', 'prob': 0.9300714}, None, None, None, {'label': 'ne', 'prob': 0.97778463}, {'label': 'ne', 'prob': 0.953232}, {'label': 'ne', 'prob': 0.9445231}, {'label': 'ne', 'prob': 0.9912242}, {'label': 'ne', 'prob': 0.98827225}, {'label': 'ne', 'prob': 0.9942405}, {'label': 'ne', 'prob': 0.9880422}, {'label': 'ne', 'prob': 0.990348}, {'label': 'ne', 'prob': 0.9776801}, {'label': 'ne', 'prob': 0.99610317}, {'label': 'ne', 'prob': 0.99483174}, {'label': 'ne', 'prob': 0.99371034}, {'label': 'ne', 'prob': 0.98250586}, {'label': 'ne', 'prob': 0.989531}, {'label': 'ne', 'prob': 0.9940134}, {'label': 'ne', 'prob': 0.9936762}, {'label': 'ne', 'prob': 0.97287524}, {'label': 'ne', 'prob': 0.9952226}, {'label': 'ne', 'prob': 0.97072214}, {'label': 'ne', 'prob': 0.98585474}, {'label': 'ne', 'prob': 0.9925048}, {'label': 'ne', 'prob': 0.92769367}, {'label': 'ne', 'prob': 0.97134525}, {'label': 'ne', 'prob': 0.9301743}, {'label': 'ne', 'prob': 0.9920626}, {'label': 'ne', 'prob': 0.92663467}, {'label': 'ne', 'prob': 0.9258245}, {'label': 'ne', 'prob': 0.99180794}, {'label': 'ne', 'prob': 0.84239984}, {'label': 'ne', 'prob': 0.9934611}, {'label': 'ne', 'prob': 0.9952483}, {'label': 'ne', 'prob': 0.98707885}, {'label': 'ne', 'prob': 0.83773446}, {'label': 'ne', 'prob': 0.9927902}, {'label': 'ne', 'prob': 0.9585371}, {'label': 'ne', 'prob': 0.9928237}, {'label': 'ne', 'prob': 0.8998495}, {'label': 'ne', 'prob': 0.86024684}, {'label': 'ne', 'prob': 0.98202986}, {'label': 'ne', 'prob': 0.96123457}, {'label': 'ne', 'prob': 0.97707975}, {'label': 'ne', 'prob': 0.99031293}, {'label': 'ne', 'prob': 0.98233396}, {'label': 'ne', 'prob': 0.98918706}, {'label': 'ne', 'prob': 0.80744666}, {'label': 'ne', 'prob': 0.89540225}, {'label': 'ne', 'prob': 0.9820052}, {'label': 'ne', 'prob': 0.9564079}, {'label': 'ne', 'prob': 0.9726224}, {'label': 'ne', 'prob': 0.9818964}, {'label': 'ne', 'prob': 0.9566217}, {'label': 'ne', 'prob': 0.9546592}, {'label': 'ne', 'prob': 0.96494603}, {'label': 'ne', 'prob': 0.9020248}, {'label': 'ne', 'prob': 0.99119353}, {'label': 'ne', 'prob': 0.9672012}, {'label': 'ne', 'prob': 0.98462284}, {'label': 'ne', 'prob': 0.98712957}, {'label': 'ne', 'prob': 0.9745057}, None, {'label': 'hi', 'prob': 0.97160816}, {'label': 'ne', 'prob': 0.9214541}, {'label': 'ne', 'prob': 0.8999832}, None, {'label': 'ne', 'prob': 0.8927763}, {'label': 'ne', 'prob': 0.8456911}, {'label': 'ne', 'prob': 0.93513644}, None, {'label': 'ne', 'prob': 0.91423965}, {'label': 'ne', 'prob': 0.94364256}, None, None, None, {'label': 'ne', 'prob': 0.9796488}, None, None, {'label': 'ne', 'prob': 0.97063833}, {'label': 'ne', 'prob': 0.8999832}, {'label': 'hi', 'prob': 0.8678507}, {'label': 'mr', 'prob': 0.839196}, {'label': 'ne', 'prob': 0.9680818}, {'label': 'ne', 'prob': 0.97876996}, None, None, {'label': 'hi', 'prob': 0.95742697}, None, {'label': 'ne', 'prob': 0.9436511}, None, {'label': 'ne', 'prob': 0.8872237}, None, None, {'label': 'ne', 'prob': 0.99592274}, {'label': 'hi', 'prob': 0.81357235}, {'label': 'ne', 'prob': 0.98102325}, None, {'label': 'ne', 'prob': 0.8543571}, {'label': 'ne', 'prob': 0.9807422}, {'label': 'ne', 'prob': 0.86411166}, None, {'label': 'hi', 'prob': 0.9192149}, {'label': 'ne', 'prob': 0.8895172}, None, {'label': 'ne', 'prob': 0.97201145}, {'label': 'hi', 'prob': 0.95249313}, {'label': 'hi', 'prob': 0.9765369}, {'label': 'hi', 'prob': 0.966933}, None, None, None, None, None, None, {'label': 'mr', 'prob': 0.90985745}, {'label': 'hi', 'prob': 0.91412675}, {'label': 'hi', 'prob': 0.95249313}, {'label': 'hi', 'prob': 0.9765369}, {'label': 'hi', 'prob': 0.966933}, None, {'label': 'hi', 'prob': 0.97906864}, None, {'label': 'ne', 'prob': 0.93964225}, {'label': 'hi', 'prob': 0.8063854}, {'label': 'hi', 'prob': 0.9755216}, None, None]}
