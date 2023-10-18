# BeerMaverickScraper project
Scraps data referenced in [beermaverick.com](https://beermaverick.com) infamous website.

This collection of tools aims to scrap data out of the beermaverick website, where a lot of carefully and thoroughly collected data is available for anyone to use (for reference).
Retrieved data types are :
* hops
* yeasts
* beer styles
* fermentables
* water profiles and adjuncts (not sure I'll do this one, this is not a resource that I'm actually using).

All credit goes to Chris Cagle for this excellent website and the amount of work behind it.
As advertised on the website, some data are published by courtesy of their respective manufacturers (hops, yeasts, malt brand names, etc.)

# Why scraping data ?
The website's database is closed, meaning there is no public access to it (which is to be expected, having a database in the public domain can prove hard to secure and can potentially generate undesired traffic).
So in order to collect the data publicly displayed on the website, you can either copy it manually (very labor intensive, error prone, etc) or use a robot to help you with that task.

# How to use this tool ?
It's very straight forward :
## Prepare your environment
```bash
# Create a virtual env, Optional but recommended (on some distributions system package manager won't let you install packages manually as they can disturb the distribution's behavior)
python -m venv .venv

# On Linux systems / bash interpreters
source .venv/bin/activate

# Install required dependencies
pip -r requirements.txt

# Launch the extraction toolset as a module,
# the "num_jobs" parameter lets you specify the amount of tasks/threads to be run in parallel (note : this is not a multi-core operation, as per Python's threading and asyncio behaviors)
# If this parameter is not given, num_jobs will be automatically derived from the amount of CPU Cores (it's not really relevant in this context but at least it'll do stuff in parallel !)
python -m Sources.Main <num_jobs>

# Output data is written in Sources/.cache/*.json
```

# Run the tests
This collection of tools also comes with some tests, in order to check that the base layers are OK.
It's not an exhaustive collection of tests by any means, but is helped stabilize the development process.

```bash
pytest Sources
```