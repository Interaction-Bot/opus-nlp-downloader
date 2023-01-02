# opus-nlp-downloader

Download aligned text automatly.

# Installation

1) Download from github: `git clone https://github.com/Interaction-Bot/opus-nlp-downloader.git`
2) Go to the directory with `cd opus-nlp-downloader`
3) Install dependency with `pip install -r requirements.txt`

# Usage

## Cli

Get datasets: `python main.py get src tgt (optional: --max_corpus)`.
exemple: `python main.py get en fr`

Download datasets: `python main.py download src tgt path (optional: --max_corpus) (optional: --max_sentences)`.
exemple: `python main.py download en fr data/`

## Python

```py
from main import *

opus = Opus()
opus.get('en', 'ab')
opus.download('data')
```