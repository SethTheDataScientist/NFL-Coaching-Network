# NFL-Coaching-Network
Developed an NFL Coaching Network and did some analysis based on potential under-rated candidates for Head coach, Offensive coordinator, and Defensive Coordinator

Project Overview
- This project at a high level has scraped wikipedia for all coaching staff data I could find to create a robust coaching network for further analysis.
- I have used this network to create some metrics to try and assess coaching quality based on the staff around you and what you might have learned from them.
- Finally, I was able to generate potential coaching staffs that a prospective Head Coach might select based on their personal coaching network
- For more in-depth information about the project, check out my portfolio: https://sites.google.com/view/seth-lanza-portfolio/home

Repo Outline
- Scrapers
  - These are the python files and outputs from scraping Wikipedia for the coaching staff records (if you know of a cleaner/better resource, let me know)
  - The scraped files folder is more where extra files went, the scraper_retry_from_csv_debug.py is the main file to try and scrape new data, and the other python files are for formatting
  - The nfl_staff_updated_OC is after cleaning and manual adjustments have been done (there is still more that could be done), a noteable inclusion is playcalling HC's are also listed as coordinator level. This is to try and get more credit for the direct reports of doing both jobs.
- R Code
  - The R files are me setting up the remaining files and dataframes needed to complete the analysis. There are plenty of cases where it is probably referencing a local file that isn't in this repo directly. The setup_final_df is the main orchestration file.
- Output
  - The outputs folder contains the saved CSVs of candidates based on the final composite metric, as well as some of the top 2026 HC cycle staff recommendations and a comparison between personal quality and hypothetical staff quality.
  - This also contains the r files to create said staffs and aggregate together.
