The goal of this project is to be able to track drama on Bluesky in real time. To do so, I intend to do the following: 

1. Triangulate drama by determining large numbers of blocks.
2. Find long chains of replies or reposts, which are likely signs of an ongoing argument. 
3. Perform sentiment analysis (in early phases of project) to find particularly argumentative or confrontational users.
4. Keep running tally of week's drama by various metrics (angriest users, highest number of blocks, etc). 

Currently, to run, just download the repository, navigate to the proper directory on your computer, and run "uvicorn slaptracker:app" on your terminal. 
