writeDB.py
- --config (required): takes in config .json file to access db server
- --dir (mutually exclusive): path to directory containing the .JSON gtx motion files to use
- --jsonFrames (mutually exclusive): path to single .JSON gtx motion file to use
- parses motion json file(s) and inserts into gtx.motion_frames
- improvement: changing info in line 170 & 171

snapshots.py
- --config (required): takes in config .json file to access db server
- Prompts user for directory that contains .json snapshots
- Parses through snapshot jsons and inserts data into gtx.snapshots_qa
- Improvement: taking directory that contains .json snapshots as arguments rather than prompting user

sessions.py
- --config (required): takes in config .json file to access db server
- Queries gtx.snapshots_qa table and seperates data into sessions
- Inserts data into gtx.upwork_sessions_qa
- Improvement: n/a

sessionSummary.py
- --config (required): takes in config .json file to access db server
- Queries gtx.snapshots_qa table and creates summary of sessoins for throught out day
- Combines session summary data with upwork_billed table
- Inserts result into gtx.upwork_daily_summary_qa
- Improvement: n/a

obsTimings.py
- --config (required): takes in config .json file to access db server
- Queries observer_timings table to calculate efficiency measurements such as idle time
- Inserts results into observers_efficiency_qa
- Old version for old data that don't contain information about position or speed events

obsTimings2.0.py
- --config (required): takes in config .json file to access db server
- Queries observer_timings table to calculate efficiency measurements such as idle time
- Inserts results into observers_efficiency_qa
- 2nd version for new data that contain information about position or speed events
- Improvement: not tested; likely to contain errors