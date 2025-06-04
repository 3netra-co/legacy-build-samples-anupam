# Site Crawler (Legacy Build Sample)

This script crawls a given website and outputs:

- All internal and external links  
- Image sources and metadata  
- Full DOM-level metadata including page titles, meta tags, and visible text  
- Crawl is restricted to a maximum page depth of 3  
- You can modify the `should_crawl` function to adjust crawling logic as needed  
- Output CSV files will be stored in the `output/` folder  
- Sample output files are available in the `sample_output/` folder  

---

## Setup

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
playwright install     # required once to download browser binaries
```

## Usage

```bash
python crawler.py --url https://example.com
```
